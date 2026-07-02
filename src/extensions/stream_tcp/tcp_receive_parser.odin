package stream_tcp

// TCP is a byte stream: one recv can contain a partial header, a partial
// payload, one complete frame, or many coalesced frames. Tcp_Receive_Parser
// turns arbitrary recv chunks into complete stream frames and appends them to
// the receive Term_Buffer.
//
// Fast path: if a complete frame is already present in the reactor buffer, the
// parser appends it directly to the receive term buffer. This keeps the receive
// path to one controlled userspace copy.
//
// Slow path: if TCP splits a frame across recv completions, the parser stages
// the incomplete frame in frame_buffer. That path necessarily performs an extra
// temporary copy, but only for split frames.

Tcp_Parse_State :: enum u8 {
    reading_header,
    reading_payload,
    complete_pending_append,
}

Tcp_Receive_Error :: enum u8 {
    none,
    malformed_frame,
    frame_too_large,
    receive_back_pressured,
}

Tcp_Receive_Result :: struct {
    consumed:        u32,
    frames_appended: u32,
    error:           Tcp_Receive_Error,
}

Tcp_Receive_Parser :: struct {
    state: Tcp_Parse_State,

    header_buf:   [FRAME_HEADER_LENGTH]u8,
    header_bytes: u32,

    current_header: Frame_Header,
    frame_buffer:   []u8,
    frame_bytes:    u32,
}

tcp_receive_parser_init :: proc(frame_buffer: []u8) -> Tcp_Receive_Parser {
    return Tcp_Receive_Parser{
        state = .reading_header,
        frame_buffer = frame_buffer,
    }
}

tcp_receive_push :: proc(parser: ^Tcp_Receive_Parser, sub: ^Subscription, bytes: []u8) -> Tcp_Receive_Result {
    result := Tcp_Receive_Result{}
    cursor: u32 = 0
    available := u32(len(bytes))

    for cursor < available || parser.state == .complete_pending_append {
        if parser.state == .complete_pending_append {
            append_result := receive_append_frame(sub, parser.frame_buffer[:parser.frame_bytes])
            if append_result == .back_pressured {
                result.error = .receive_back_pressured
                return result
            }
            if append_result != .none {
                tcp_receive_parser_reset(parser)
                result.error = .malformed_frame
                return result
            }

            result.frames_appended += 1
            tcp_receive_parser_reset(parser)
            continue
        }

        if parser.state == .reading_header && parser.header_bytes == 0 && available - cursor >= FRAME_HEADER_LENGTH {
            header := read_header(bytes, cursor)
            validation := validate_header(header, u32(len(parser.frame_buffer)))
            if validation != .none {
                result.error = validation
                return result
            }

            frame_length := header.frame_length
            if available - cursor >= frame_length {
                append_result := receive_append_frame(sub, bytes[cursor:cursor + frame_length])
                if append_result == .back_pressured {
                    result.error = .receive_back_pressured
                    return result
                }
                if append_result != .none {
                    result.error = .malformed_frame
                    return result
                }

                cursor += frame_length
                result.consumed = cursor
                result.frames_appended += 1
                continue
            }

            // Partial frame: stage header and whatever payload bytes are present.
            copy_bytes(parser.frame_buffer[:FRAME_HEADER_LENGTH], bytes[cursor:cursor + FRAME_HEADER_LENGTH])
            parser.current_header = header
            parser.header_bytes = FRAME_HEADER_LENGTH
            parser.frame_bytes = FRAME_HEADER_LENGTH
            parser.state = .reading_payload
            cursor += FRAME_HEADER_LENGTH
            result.consumed = cursor
        }

        if parser.state == .reading_header {
            needed := FRAME_HEADER_LENGTH - parser.header_bytes
            take := min_u32(needed, available - cursor)
            copy_bytes(parser.header_buf[parser.header_bytes:parser.header_bytes + take], bytes[cursor:cursor + take])
            parser.header_bytes += take
            cursor += take
            result.consumed = cursor

            if parser.header_bytes < FRAME_HEADER_LENGTH {
                return result
            }

            header := read_header(parser.header_buf[:], 0)
            validation := validate_header(header, u32(len(parser.frame_buffer)))
            if validation != .none {
                tcp_receive_parser_reset(parser)
                result.error = validation
                return result
            }

            copy_bytes(parser.frame_buffer[:FRAME_HEADER_LENGTH], parser.header_buf[:])
            parser.current_header = header
            parser.frame_bytes = FRAME_HEADER_LENGTH
            parser.state = .reading_payload
        }

        if parser.state == .reading_payload {
            target_length := parser.current_header.frame_length
            needed := target_length - parser.frame_bytes
            take := min_u32(needed, available - cursor)

            if take > 0 {
                copy_bytes(parser.frame_buffer[parser.frame_bytes:parser.frame_bytes + take], bytes[cursor:cursor + take])
                parser.frame_bytes += take
                cursor += take
                result.consumed = cursor
            }

            if parser.frame_bytes < target_length {
                return result
            }

            parser.state = .complete_pending_append
        }
    }

    return result
}

tcp_receive_parser_reset :: proc(parser: ^Tcp_Receive_Parser) {
    parser.state = .reading_header
    parser.header_bytes = 0
    parser.frame_bytes = 0
    parser.current_header = Frame_Header{}
}

validate_header :: proc(header: Frame_Header, max_frame_length: u32) -> Tcp_Receive_Error {
    if header.frame_length < FRAME_HEADER_LENGTH {
        return .malformed_frame
    }

    if header.frame_length > max_frame_length {
        return .frame_too_large
    }

    if header.frame_type != u16(Frame_Type.DATA) && header.frame_type != u16(Frame_Type.PADDING) {
        return .malformed_frame
    }

    return .none
}

min_u32 :: proc(a: u32, b: u32) -> u32 {
    if a < b {
        return a
    }
    return b
}
