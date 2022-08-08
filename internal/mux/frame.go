package mux

import (
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

const (
	flagFinal = 1 << iota // stream is being closed gracefully
	flagError             // stream is being closed due to an error
)

const (
	idErrorBadInit        = iota // should never be seen
	idEstablishEncryption        // encryption handshake frame
	idUpdateSettings             // settings handshake frame
	idKeepalive                  // empty frame to keep connection open
)

type frameHeader struct {
	id     uint32
	length uint32
	flags  uint16
}

const frameHeaderSize = 10
const encryptedHeaderSize = frameHeaderSize + chachaOverhead

func encodeFrameHeader(buf []byte, h frameHeader) {
	binary.LittleEndian.PutUint32(buf[0:], h.id)
	binary.LittleEndian.PutUint32(buf[4:], h.length)
	binary.LittleEndian.PutUint16(buf[8:], h.flags)
}

func decodeFrameHeader(buf []byte) (h frameHeader) {
	h.id = binary.LittleEndian.Uint32(buf[0:])
	h.length = binary.LittleEndian.Uint32(buf[4:])
	h.flags = binary.LittleEndian.Uint16(buf[8:])
	return
}

func readFrame(r io.Reader, buf []byte) (frameHeader, []byte, error) {
	// read and decode header
	if _, err := io.ReadFull(r, buf[:frameHeaderSize]); err != nil {
		return frameHeader{}, nil, err
	}
	h := decodeFrameHeader(buf)
	if h.length > uint32(len(buf)) {
		return frameHeader{}, nil, errors.New("peer sent too-large unencrypted frame")
	}
	// read payload
	payload := buf[:h.length]
	if _, err := io.ReadFull(r, payload); err != nil {
		return frameHeader{}, nil, err
	}
	if h.flags&flagError != 0 {
		return h, nil, errors.New(string(payload))
	}
	return h, payload, nil
}

var ourVersion = []byte{1}

func initiateVersionHandshake(conn net.Conn) error {
	theirVersion := make([]byte, 1)
	if _, err := conn.Write(ourVersion); err != nil {
		return fmt.Errorf("could not write our version: %w", err)
	} else if _, err := io.ReadFull(conn, theirVersion); err != nil {
		return fmt.Errorf("could not read peer version: %w", err)
	} else if theirVersion[0] != 1 {
		return errors.New("bad version")
	}
	return nil
}

func acceptVersionHandshake(conn net.Conn) error {
	theirVersion := make([]byte, 1)
	if _, err := io.ReadFull(conn, theirVersion); err != nil {
		return fmt.Errorf("could not read peer version: %w", err)
	} else if _, err := conn.Write(ourVersion); err != nil {
		return fmt.Errorf("could not write our version: %w", err)
	} else if theirVersion[0] != 1 {
		return errors.New("bad version")
	}
	return nil
}

type connSettings struct {
	RequestedPacketSize int
	MaxFrameSizePackets int
	MaxTimeout          time.Duration
}

func (cs connSettings) maxFrameSize() int {
	return cs.MaxFrameSizePackets * cs.RequestedPacketSize
}

func (cs connSettings) maxPayloadSize() int {
	return cs.maxFrameSize() - encryptedHeaderSize - chachaOverhead
}

const connSettingsSize = 24

var defaultConnSettings = connSettings{
	RequestedPacketSize: 1440, // IPv6 MTU
	MaxFrameSizePackets: 10,
	MaxTimeout:          20 * time.Minute,
}

func initiateSettingsHandshake(conn net.Conn, ours connSettings, aead cipher.AEAD) (connSettings, error) {
	// encode + write request
	frameBuf := make([]byte, ours.RequestedPacketSize)
	payload := make([]byte, connSettingsSize)
	binary.LittleEndian.PutUint64(payload[0:], uint64(ours.RequestedPacketSize))
	binary.LittleEndian.PutUint64(payload[8:], uint64(ours.MaxFrameSizePackets))
	binary.LittleEndian.PutUint64(payload[16:], uint64(ours.MaxTimeout.Seconds()))
	frame := encryptFrame(frameBuf, frameHeader{
		id:     idUpdateSettings,
		length: uint32(len(payload)),
	}, payload, ours.RequestedPacketSize, aead)
	if _, err := conn.Write(frame); err != nil {
		return connSettings{}, err
	}
	// read + decode response
	h, payload, err := readEncryptedFrame(conn, frameBuf, ours.RequestedPacketSize, aead)
	if err != nil {
		return connSettings{}, err
	} else if h.id != idUpdateSettings {
		return connSettings{}, errors.New("invalid settings ID")
	} else if h.length != connSettingsSize {
		return connSettings{}, errors.New("invalid settings payload")
	}
	theirs := connSettings{
		RequestedPacketSize: int(binary.LittleEndian.Uint64(payload[0:])),
		MaxFrameSizePackets: int(binary.LittleEndian.Uint64(payload[8:])),
		MaxTimeout:          time.Second * time.Duration(binary.LittleEndian.Uint64(payload[16:])),
	}
	return mergeSettings(ours, theirs)
}

func acceptSettingsHandshake(conn net.Conn, ours connSettings, aead cipher.AEAD) (connSettings, error) {
	// read + decode request
	frameBuf := make([]byte, ours.RequestedPacketSize)
	h, payload, err := readEncryptedFrame(conn, frameBuf, ours.RequestedPacketSize, aead)
	if err != nil {
		return connSettings{}, err
	} else if h.id != idUpdateSettings {
		return connSettings{}, errors.New("invalid settings ID")
	} else if h.length != connSettingsSize {
		return connSettings{}, errors.New("invalid settings payload")
	}
	theirs := connSettings{
		RequestedPacketSize: int(binary.LittleEndian.Uint64(payload[0:])),
		MaxFrameSizePackets: int(binary.LittleEndian.Uint64(payload[8:])),
		MaxTimeout:          time.Second * time.Duration(binary.LittleEndian.Uint64(payload[16:])),
	}
	// encode + write response
	payload = make([]byte, connSettingsSize)
	binary.LittleEndian.PutUint64(payload[0:], uint64(ours.RequestedPacketSize))
	binary.LittleEndian.PutUint64(payload[8:], uint64(ours.MaxFrameSizePackets))
	binary.LittleEndian.PutUint64(payload[16:], uint64(ours.MaxTimeout.Seconds()))
	frame := encryptFrame(frameBuf, frameHeader{
		id:     idUpdateSettings,
		length: uint32(len(payload)),
	}, payload, ours.RequestedPacketSize, aead)
	if _, err := conn.Write(frame); err != nil {
		return connSettings{}, err
	}
	return mergeSettings(ours, theirs)
}

func mergeSettings(ours, theirs connSettings) (connSettings, error) {
	// use smaller value for all settings
	merged := ours
	if theirs.RequestedPacketSize < merged.RequestedPacketSize {
		merged.RequestedPacketSize = theirs.RequestedPacketSize
	}
	if theirs.MaxFrameSizePackets < merged.MaxFrameSizePackets {
		merged.MaxFrameSizePackets = theirs.MaxFrameSizePackets
	}
	if theirs.MaxTimeout < merged.MaxTimeout {
		merged.MaxTimeout = theirs.MaxTimeout
	}
	// enforce minimums and maximums
	switch {
	case merged.RequestedPacketSize < 1220:
		return connSettings{}, errors.New("requested packet size is too small")
	case merged.MaxFrameSizePackets < 10:
		return connSettings{}, errors.New("maximum frame size is too small")
	case merged.MaxFrameSizePackets > 64:
		return connSettings{}, errors.New("maximum frame size is too large")
	case merged.MaxTimeout < 2*time.Minute:
		return connSettings{}, errors.New("maximum timeout is too short")
	}
	return merged, nil
}
