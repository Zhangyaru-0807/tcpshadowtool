package main

import (
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

type PgFortuneBackend struct {
	backend   *pgproto3.Backend
	conn      net.Conn
	responder func() ([]byte, error)
}

func NewPgFortuneBackend(conn net.Conn, responder func() ([]byte, error)) *PgFortuneBackend {
	backend := pgproto3.NewBackend(conn, conn)

	connHandler := &PgFortuneBackend{
		backend:   backend,
		conn:      conn,
		responder: responder,
	}

	return connHandler
}

func (p *PgFortuneBackend) Run() error {
	defer p.Close()

	//启动
	err := p.handleStartup()
	if err != nil {
		return err
	}

	for {
		msg, err := p.backend.Receive()
		if err != nil {
			return fmt.Errorf("error receiving message: %w", err)
		}

		switch msg.(type) {
		case *pgproto3.Query:
			response, err := p.responder()
			if err != nil {
				return fmt.Errorf("error generating query response: %w", err)
			}

			buf := (&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
				{
					Name:                 []byte("fortune"),
					TableOID:             0,
					TableAttributeNumber: 0,
					DataTypeOID:          25,
					DataTypeSize:         -1,
					TypeModifier:         -1,
					Format:               0,
				},
			}}).Encode(nil)
			buf = (&pgproto3.DataRow{Values: [][]byte{response}}).Encode(buf)
			buf = (&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")}).Encode(buf)
			buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
			_, err = p.conn.Write(buf)
			if err != nil {
				return fmt.Errorf("error writing query response: %w", err)
			}
		case *pgproto3.Terminate:
			return nil
		default:
			return fmt.Errorf("received message other than Query from client: %#v", msg)
		}
	}
}

// 句柄启动
func (p *PgFortuneBackend) handleStartup() error {
	startupMessage, err := p.backend.ReceiveStartupMessage()
	if err != nil {
		return fmt.Errorf("error receiving startup message: %w", err)
	}

	var auth []string
	auth = append(auth, "SCRAM-SHA-256")
	switch startupMessage.(type) {
	case *pgproto3.StartupMessage:
		buf := (&pgproto3.AuthenticationSASL{AuthMechanisms: auth}).Encode(nil)
		_, err = p.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	case *pgproto3.SSLRequest:
		_, err = p.conn.Write([]byte("N"))
		if err != nil {
			return fmt.Errorf("error sending deny SSL request: %w", err)
		}
		return p.handleStartup()
	case *pgproto3.SASLInitialResponse:
		//buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		//buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		buf := (&pgproto3.AuthenticationSASLContinue{}).Encode(nil)
		_, err = p.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	case *pgproto3.SASLResponse:
		//buf := (&pgproto3.AuthenticationOk{}).Encode(nil)
		//buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		buf := (&pgproto3.AuthenticationSASLFinal{}).Encode(nil)
		buf = (&pgproto3.AuthenticationOk{}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "application_name"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, YMD"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "default_transaction_read_only", Value: "off"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "in_hot_standby", Value: "off"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "integer_datetimes", Value: "on"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "IntervalStyle", Value: "postgres"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "is_superuser", Value: "on"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "server_encoding", Value: "UTF8"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "server_version", Value: "14.5"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "session_authorization", Value: "postgres"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"}).Encode(buf)
		buf = (&pgproto3.ParameterStatus{Name: "TimeZone", Value: "Asia/Shanghai"}).Encode(buf)
		buf = (&pgproto3.BackendKeyData{ProcessID: 9920, SecretKey: 1678171750}).Encode(buf)
		buf = (&pgproto3.ReadyForQuery{TxStatus: 'I'}).Encode(buf)
		_, err = p.conn.Write(buf)
		if err != nil {
			return fmt.Errorf("error sending ready for query: %w", err)
		}
	default:
		return fmt.Errorf("unknown startup message: %#v", startupMessage)
	}

	return nil
}

func (p *PgFortuneBackend) Close() error {
	return p.conn.Close()
}
