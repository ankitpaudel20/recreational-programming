package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
)

func panic_error(err error) {
	if err != nil {
		fmt.Println("error occured:", err)
		panic(err)
	}
}

func assert(cond bool, format_string string, args ...any) {
	if !cond {
		panic(fmt.Sprintf(format_string, args...))
	}
}

func assertIfError(err error, format_string string, args ...any) {
	if err != nil {
		has_placeholder := strings.Index(format_string, "%e")
		if has_placeholder != -1 {
			format_string = format_string[:has_placeholder] + err.Error() + format_string[has_placeholder+2:]
		}
		panic(fmt.Sprintf(format_string, args...))
	}
}

func encode_command(cmd string) []byte {
	words := strings.Fields(cmd)
	CLRF := "\r\n"
	buf := make([]byte, 0, 256)
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(words)), 10)
	buf = append(buf, CLRF...)
	for _, word := range words {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(word)), 10)
		buf = append(buf, CLRF...)
		buf = append(buf, word...)
		buf = append(buf, CLRF...)
	}
	return buf
}

var CLRF = []byte{'\r', '\n'}

func __discard_last_delimiter_redis(res *bufio.Reader) {
	_, err := res.Discard(len(CLRF))
	assertIfError(err, "Error in parsing RESP: could not find the end of the command| %e")

}

func __get_initial_len_redis(res *bufio.Reader) int {
	data, err := res.ReadBytes('\r')
	res.Discard(1)
	assertIfError(err, "Error in parsing RESP: did not find the data delimiter | %e")
	string_len, err := strconv.Atoi(string(data[:len(data)-1]))
	assertIfError(err, "Error in parsing RESP: error reading length of next data| %e")
	return string_len
}

func decode_response(resp *bufio.Reader) string {

	first_byte, err := resp.ReadByte()
	assertIfError(err, "Error in parsing RESP: no byte received from server | %e")

	switch first_byte {
	case '_':
		return "NULL"
	case '#':
		next_byte, err := resp.ReadByte()
		assertIfError(err, "Error in parsing RESP: can't read bool | %e")
		if next_byte == 't' {
			return "true"
		}
		return "false"
	case '$':
		string_len := __get_initial_len_redis(resp)
		if string_len == -1 {
			return "NIL"
		}
		data := make([]byte, string_len)
		_, err := io.ReadFull(resp, data)

		assertIfError(err, "Error in parsing RESP: could not find the end of the command| %e")
		__discard_last_delimiter_redis(resp)
		return string(data)
	case '*':
		array_len := __get_initial_len_redis(resp)
		if array_len < 0 {
			return "[NIL]"
		}
		if array_len == 1 {
			return "[]"
		}
		ret := "["
		for i := 0; i < array_len; i++ {
			decoded_data := decode_response(resp)
			ret = ret + decoded_data + ","
		}
		ret += "]"
		__discard_last_delimiter_redis(resp)
		return ret
	case '%':
		map_len := __get_initial_len_redis(resp)
		res := ""
		for _ = range map_len {
			res += (decode_response(resp) + "==>" + decode_response(resp))
		}
		__discard_last_delimiter_redis(resp)
		return res
	case ':':
		number := __get_initial_len_redis(resp)
		return strconv.Itoa(number)
	case '-':
		fallthrough
	case '+':
		data, err := resp.ReadBytes('\r')
		resp.Discard(1)
		assertIfError(err, "Error in parsing RESP: could not find the end of the command| %e")
		return string(data)
	}
	assert(false, "code should not reach here, pls check the decode function")
	return ""
}

type RedisData struct {
	command   []byte
	recv_chan chan *bufio.Reader
}

type Redis struct {
	// conn         net.Conn
	conn_pool *ResourcePool[*bufio.ReadWriter]
}

func (redis *Redis) RunCommandNew(command string) (string, error) {
	read_writer := redis.conn_pool.Get()
	defer redis.conn_pool.Release(read_writer)
	read_writer.Write(encode_command(command))
	err := read_writer.Flush()
	if err != nil {
		return "", err
	}
	response := decode_response(read_writer.Reader)
	return response, nil
}

func RedisFromURI(uri string) *Redis {
	u, err := url.Parse(uri)
	assertIfError(err, "invalid redis uri | '%e'")
	assert(u.Scheme == "redis", "invalid uri scheme, must be set to 'redis'")
	initializer_func := func() (*bufio.ReadWriter, error) {
		conn, err := net.Dial("tcp", u.Host)
		assertIfError(err, "can't connect to redis, error: '%e'")
		pass, has_pass := u.User.Password()
		if has_pass {
			cmd := encode_command(fmt.Sprintf("auth %s %s", u.User.Username(), pass))
			conn.Write(cmd)
			temp_buf := make([]byte, 1024)
			n, err := conn.Read(temp_buf)
			assertIfError(err, "error reading response from redis, error: '%e'")
			expected_response := []byte("+OK\r\n")
			if n != len(expected_response) {
				return nil, errors.New(fmt.Sprintf("error authenticating response from redis, error: '%s'", temp_buf))
			}

		}
		if u.Path != "" {
			split_path := strings.Split(u.Path, "/")
			assert(len(split_path) == 2, "you can only have a db selection, got paths: '%s'", u.Path)
			cmd := encode_command(fmt.Sprintf("auth %s %s", u.User.Username(), pass))
			conn.Write(cmd)
			temp_buf := make([]byte, 1024)
			_, _ = conn.Read(temp_buf)
		}

		temp := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		return temp, nil
	}
	pool, err := NewConnectionPool(1, 10, initializer_func)
	assertIfError(err, "error initializing client '%e'")

	redis := Redis{pool}

	return &redis
}

func main() {
	// command_string := "ping hello"
	redis := RedisFromURI("redis://:asdf1234@localhost:6379/10")
	fmt.Printf("connected to redis, pinging it.\n")
	resp, _ := redis.RunCommandNew("ping hello")
	fmt.Println(resp)
	res, _ := redis.RunCommandNew("set hello world")
	fmt.Printf("%s\n", res)
	res, _ = redis.RunCommandNew("llen esd")
	fmt.Printf("%s\n", res)
}
