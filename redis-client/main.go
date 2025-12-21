package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
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

func decode_response(resp []byte) string {
	getlen := func(temp []byte) (int, error) {
		first_delim := bytes.Index(temp, []byte{'\r', '\n'})
		return strconv.Atoi(string(temp[1:first_delim]))
	}

	if len(resp) == 0 {
		return ""
	}

	switch resp[0] {
	case '-':
		fallthrough
	case '+':
		fallthrough
	case ':':
		return string(resp[1 : len(resp)-2])
	case '$':
		if resp[1] == '-' {
			return ""
		}
		data := bytes.SplitN(resp, []byte{'\r', '\n'}, 2)
		return string(data[1])
	case '*':
		n, err := getlen(resp)
		first_delim := bytes.Index(resp, []byte{'\r', '\n'})
		assertIfError(err, "error while parsing array in redis response | %e")
		ret := "["
		for i := 0; i < n; i++ {
			ret = ret + decode_response(resp[first_delim+2:]) + ","
		}
		ret += "]"
		return ret
	}
	assert(false, "code should not reach here, pls check the decode function")
	return ""
}

type Redis struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}


func authenticate(redis Redis, username string, password string) {
	recv_data := redis.RunCommand( fmt.Sprintf("auth %s %s", username, password))
	decoded_resp := decode_response([]byte(recv_data))
	assert(decoded_resp == "OK", "error authenticating: '%s'", recv_data)
}

func (redis * Redis)RunCommand(command string) []byte {
	encoded_command := encode_command(command)
	redis.conn.Write(encoded_command)
	recv_buffer := make([]byte, 1024)
	n, err := redis.conn.Read(recv_buffer)
	assertIfError(err, "error running command %s: '%e'", command)
	assert(n != 0, "error running command %s | could not read from socket", command)
	return recv_buffer[:n]
}

func RedisFromURI(uri string) Redis {
	u, err := url.Parse(uri)
	assertIfError(err, "invalid redis uri | '%e'")
	assert(u.Scheme == "redis", "invalid uri scheme, must be set to 'redis'")
	conn, err := net.Dial("tcp", u.Host)

	assertIfError(err, "can't connect to redis, error: '%e'")
	redis := Redis{conn, bufio.NewReader(conn), bufio.NewWriter(conn)}
	pass, has_pass := u.User.Password()
	if has_pass {
		authenticate(redis, u.User.Username(), pass)
	}

	if u.Path != "" {
		split_path := strings.Split(u.Path, "/")
		assert(len(split_path) == 2, "you can only have a db selection, got paths: '%s'", u.Path)
		redis.RunCommand( "select "+split_path[1])
	}
	return redis
}

func main() {
	// create_listen_server()
	// command_string := "ping hello"
	redis := RedisFromURI("redis://:asdf1234@localhost:6379/10")
	fmt.Printf("connected to redis, pinging it.\n")
	resp := decode_response(redis.RunCommand(, "ping hello"))
	fmt.Println(resp)
	res := redis.RunCommand( "info")
	decoded := decode_response(res)
	fmt.Printf("%s", decoded)
	// conn, err := net.Dial("tcp", "localhost:6379")
	// panic_error(err)
	// fmt.Printf("%s\n", encode_command(command_string))

	// conn.Write(encode_command(command_string))
	// read_buf := make([]byte, 256)
	// n, err := conn.Read(read_buf)
	// panic_error(err)
	// fmt.Printf("read %d bytes, data: %s\n", n, read_buf)

}

func send_hello() {
	conn, err := net.Dial("tcp", "localhost:9000")
	panic_error(err)
	for {
		fmt.Println("[Client] sending hello\n")
		conn.Write([]byte("hello"))
		time.Sleep(2 * time.Second)
	}
}

func handleConnection(conn net.Conn) {
	remote_addr := conn.RemoteAddr()
	fmt.Printf("[%v] Client got connected\n", remote_addr)
	defer conn.Close()

	var buf []byte = make([]byte, 1024)
	for {
		len, err := conn.Read(buf)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("[%v] read %d bytes | data: '%s'\n", remote_addr, len, buf)
	}
}
