package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
)

const (
	STRING  byte = '+'
	ERROR   byte = '-'
	INTEGER byte = ':'
	BULK    byte = '$'
	ARRAY   byte = '*'
)

var (
	ErrInvalidRESP = errors.New("invalid RESP")
	CRLF           = []byte{'\r', '\n'}
)

type Command struct {
	Name string
	Args [][]byte
}

type Parser struct {
	reader *bufio.Reader
}

func NewParser(r io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReader(r),
	}
}

func (p *Parser) Parse() (*Command, error) {
	typ, err := p.reader.ReadByte()
	if err != nil {
		return nil, err
	}

	switch typ {
	case ARRAY:
		return p.parseArray()
	default:
		return nil, ErrInvalidRESP
	}
}

func (p *Parser) parseArray() (*Command, error) {
	length, err := p.parseInteger()
	if err != nil {
		return nil, err
	}

	if length < 1 {
		return nil, ErrInvalidRESP
	}

	command := &Command{
		Args: make([][]byte, length),
	}

	for i := 0; i < length; i++ {
		typ, err := p.reader.ReadByte()
		if err != nil {
			return nil, err
		}

		if typ != BULK {
			return nil, ErrInvalidRESP
		}

		bulk, err := p.parseBulkString()
		if err != nil {
			return nil, err
		}

		command.Args[i] = bulk
	}

	command.Name = string(command.Args[0])
	command.Args = command.Args[1:]

	return command, nil
}

func (p *Parser) parseBulkString() ([]byte, error) {
	length, err := p.parseInteger()
	if err != nil {
		return nil, err
	}

	if length < 0 {
		return nil, nil
	}

	bulk := make([]byte, length)
	if _, err := io.ReadFull(p.reader, bulk); err != nil {
		return nil, err
	}

	crlf := make([]byte, 2)
	if _, err := io.ReadFull(p.reader, crlf); err != nil {
		return nil, err
	}

	if !bytes.Equal(crlf, CRLF) {
		return nil, ErrInvalidRESP
	}

	return bulk, nil
}

func (p *Parser) parseInteger() (int, error) {
	line, err := p.reader.ReadString('\n')
	if err != nil {
		return 0, err
	}

	if len(line) < 3 || line[len(line)-2] != '\r' {
		return 0, ErrInvalidRESP
	}

	n, err := strconv.Atoi(line[:len(line)-2])
	if err != nil {
		return 0, err
	}

	return n, nil
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		writer: w,
	}
}

func (w *Writer) WriteString(s string) error {
	_, err := w.writer.Write([]byte{STRING})
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(s))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	return err
}

func (w *Writer) WriteError(err error) error {
	_, err = w.writer.Write([]byte{ERROR})
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(err.Error()))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	return err
}

func (w *Writer) WriteInteger(n int64) error {
	_, err := w.writer.Write([]byte{INTEGER})
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(strconv.FormatInt(n, 10)))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	return err
}

func (w *Writer) WriteBulk(b []byte) error {
	if b == nil {
		_, err := w.writer.Write([]byte("$-1\r\n"))
		return err
	}

	_, err := w.writer.Write([]byte{BULK})
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(strconv.Itoa(len(b))))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	if err != nil {
		return err
	}

	_, err = w.writer.Write(b)
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	return err
}

func (w *Writer) WriteArray(arr [][]byte) error {
	if arr == nil {
		_, err := w.writer.Write([]byte("$-1\r\n"))
		return err
	}

	_, err := w.writer.Write([]byte{ARRAY})
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(strconv.Itoa(len(arr))))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	if err != nil {
		return err
	}

	for _, item := range arr {
		err = w.WriteBulk(item)
		if err != nil {
			return err
		}
	}

	return nil
}
