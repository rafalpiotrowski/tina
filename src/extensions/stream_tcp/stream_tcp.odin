package stream_tcp

// Aeron-style TCP stream data plane core.
//
// This package intentionally implements only the bounded in-memory data-plane
// primitives. Tina Isolates should own/supervise these structures and perform
// actual TCP I/O through ctx_io_send/ctx_submit_io. High-volume data should not
// be routed through Tina mailboxes.

Stream_Id  :: distinct u32
Session_Id :: distinct u32
Position   :: distinct u64

FRAME_ALIGNMENT :: u32 = 8
FRAME_HEADER_LENGTH :: u32 = 32

Frame_Type :: enum u16 {
    DATA    = 1,
    PADDING = 2,
}

Frame_Flags :: enum u16 {
    NONE = 0,
}

Frame_Header :: struct {
    frame_length: u32, // header + payload, excluding alignment padding
    frame_type:   u16,
    flags:        u16,
    stream_id:    Stream_Id,
    session_id:   Session_Id,
    position:     Position,
    reserved:     u64,
}

Offer_Error :: enum u8 {
    none,
    back_pressured,
    not_connected,
    closed,
    message_too_large,
}

Offer_Result :: struct {
    position: Position,
    error:    Offer_Error,
}

Poll_Result :: enum u8 {
    ok,
    no_data,
    malformed_frame,
}

Fragment_Handler :: proc(payload: []u8, header: ^Frame_Header)

Term_Buffer :: struct {
    data: []u8,
    capacity: u32,

    // Positions are absolute and monotonic. Indices are position % capacity.
    write_position: Position,
    read_position:  Position,
}

Publication :: struct {
    stream_id:  Stream_Id,
    session_id: Session_Id,

    term: Term_Buffer,

    connected: bool,
    closed:    bool,

    max_payload_length: u32,

    counters: Stream_Counters,
}

Subscription :: struct {
    stream_id:  Stream_Id,
    session_id: Session_Id,

    term: Term_Buffer,

    counters: Stream_Counters,
}

Stream_Counters :: struct {
    offers_ok:              u64,
    offers_back_pressured:  u64,
    offers_not_connected:   u64,
    offers_closed:          u64,
    offers_too_large:       u64,

    frames_written:         u64,
    frames_read:            u64,
    padding_frames_written: u64,
    padding_frames_read:    u64,

    bytes_written:          u64,
    bytes_read:             u64,

    malformed_frames:       u64,
}

Drain_Slice :: struct {
    data: []u8,
    ok:   bool,
}

term_init :: proc(buffer: []u8) -> Term_Buffer {
    return Term_Buffer{
        data = buffer,
        capacity = u32(len(buffer)),
        write_position = 0,
        read_position = 0,
    }
}

publication_init :: proc(buffer: []u8, stream_id: Stream_Id, session_id: Session_Id, max_payload_length: u32) -> Publication {
    return Publication{
        stream_id = stream_id,
        session_id = session_id,
        term = term_init(buffer),
        connected = true,
        closed = false,
        max_payload_length = max_payload_length,
    }
}

subscription_init :: proc(buffer: []u8, stream_id: Stream_Id, session_id: Session_Id) -> Subscription {
    return Subscription{
        stream_id = stream_id,
        session_id = session_id,
        term = term_init(buffer),
    }
}

align_frame :: proc(length: u32) -> u32 {
    return (length + FRAME_ALIGNMENT - 1) & ~(FRAME_ALIGNMENT - 1)
}

term_used :: proc(term: ^Term_Buffer) -> u64 {
    return u64(term.write_position) - u64(term.read_position)
}

term_available :: proc(term: ^Term_Buffer) -> u64 {
    return u64(term.capacity) - term_used(term)
}

term_index :: proc(term: ^Term_Buffer, position: Position) -> u32 {
    return u32(u64(position) % u64(term.capacity))
}

publication_offer :: proc(pub: ^Publication, payload: []u8, reserved: u64 = 0) -> Offer_Result {
    if pub.closed {
        pub.counters.offers_closed += 1
        return Offer_Result{error = .closed}
    }

    if !pub.connected {
        pub.counters.offers_not_connected += 1
        return Offer_Result{error = .not_connected}
    }

    payload_length := u32(len(payload))
    if payload_length > pub.max_payload_length {
        pub.counters.offers_too_large += 1
        return Offer_Result{error = .message_too_large}
    }

    frame_length := FRAME_HEADER_LENGTH + payload_length
    aligned_length := align_frame(frame_length)

    if aligned_length > pub.term.capacity {
        pub.counters.offers_too_large += 1
        return Offer_Result{error = .message_too_large}
    }

    // Need one byte of slack to distinguish full from empty in the circular buffer.
    if term_available(&pub.term) <= u64(aligned_length) {
        pub.counters.offers_back_pressured += 1
        return Offer_Result{error = .back_pressured}
    }

    write_index := term_index(&pub.term, pub.term.write_position)
    tail_available := pub.term.capacity - write_index

    if aligned_length > tail_available {
        padding_length := tail_available
        if term_available(&pub.term) <= u64(padding_length + aligned_length) {
            pub.counters.offers_back_pressured += 1
            return Offer_Result{error = .back_pressured}
        }

        padding_header := Frame_Header{
            frame_length = padding_length,
            frame_type = u16(Frame_Type.PADDING),
            flags = u16(Frame_Flags.NONE),
            stream_id = pub.stream_id,
            session_id = pub.session_id,
            position = pub.term.write_position,
            reserved = 0,
        }
        write_header(pub.term.data, write_index, padding_header)
        zero_range(pub.term.data, write_index + FRAME_HEADER_LENGTH, padding_length - FRAME_HEADER_LENGTH)
        pub.term.write_position = Position(u64(pub.term.write_position) + u64(padding_length))
        pub.counters.padding_frames_written += 1
        write_index = 0
    }

    position := pub.term.write_position
    header := Frame_Header{
        frame_length = frame_length,
        frame_type = u16(Frame_Type.DATA),
        flags = u16(Frame_Flags.NONE),
        stream_id = pub.stream_id,
        session_id = pub.session_id,
        position = position,
        reserved = reserved,
    }

    write_header(pub.term.data, write_index, header)
    copy_bytes(pub.term.data[write_index + FRAME_HEADER_LENGTH:], payload)
    zero_range(pub.term.data, write_index + frame_length, aligned_length - frame_length)

    pub.term.write_position = Position(u64(pub.term.write_position) + u64(aligned_length))
    pub.counters.offers_ok += 1
    pub.counters.frames_written += 1
    pub.counters.bytes_written += u64(frame_length)

    return Offer_Result{position = position, error = .none}
}

// Returns contiguous framed bytes ready for TCP send. The returned slice is valid
// until publication_commit_drained mutates the read position.
publication_drain_contiguous :: proc(pub: ^Publication) -> Drain_Slice {
    if u64(pub.term.read_position) == u64(pub.term.write_position) {
        return Drain_Slice{ok = false}
    }

    read_index := term_index(&pub.term, pub.term.read_position)
    contiguous := pub.term.capacity - read_index
    available := term_used(&pub.term)
    if available < u64(contiguous) {
        contiguous = u32(available)
    }

    return Drain_Slice{data = pub.term.data[read_index:read_index + contiguous], ok = true}
}

publication_commit_drained :: proc(pub: ^Publication, byte_count: u32) {
    available := term_used(&pub.term)
    n := u64(byte_count)
    if n > available {
        n = available
    }
    pub.term.read_position = Position(u64(pub.term.read_position) + n)
}

subscription_poll :: proc(sub: ^Subscription, handler: Fragment_Handler, fragment_limit: u32) -> u32 {
    fragments: u32 = 0

    for fragments < fragment_limit && u64(sub.term.read_position) < u64(sub.term.write_position) {
        read_index := term_index(&sub.term, sub.term.read_position)

        if sub.term.capacity - read_index < FRAME_HEADER_LENGTH {
            sub.counters.malformed_frames += 1
            break
        }

        header := read_header(sub.term.data, read_index)
        if header.frame_length < FRAME_HEADER_LENGTH {
            sub.counters.malformed_frames += 1
            break
        }

        aligned_length := align_frame(header.frame_length)
        if aligned_length > sub.term.capacity {
            sub.counters.malformed_frames += 1
            break
        }

        if u64(sub.term.read_position) + u64(aligned_length) > u64(sub.term.write_position) {
            break
        }

        if header.frame_type == u16(Frame_Type.PADDING) {
            sub.term.read_position = Position(u64(sub.term.read_position) + u64(aligned_length))
            sub.counters.padding_frames_read += 1
            continue
        }

        payload_offset := read_index + FRAME_HEADER_LENGTH
        payload_length := header.frame_length - FRAME_HEADER_LENGTH
        payload := sub.term.data[payload_offset:payload_offset + payload_length]

        handler(payload, &header)

        sub.term.read_position = Position(u64(sub.term.read_position) + u64(aligned_length))
        sub.counters.frames_read += 1
        sub.counters.bytes_read += u64(header.frame_length)
        fragments += 1
    }

    return fragments
}

// Append a complete frame received from TCP into the subscription receive buffer.
// This is the one controlled userspace copy on the receive path.
receive_append_frame :: proc(sub: ^Subscription, frame: []u8) -> Offer_Error {
    if len(frame) < int(FRAME_HEADER_LENGTH) {
        sub.counters.malformed_frames += 1
        return .message_too_large
    }

    header := read_header(frame, 0)
    if header.frame_length != u32(len(frame)) || header.frame_length < FRAME_HEADER_LENGTH {
        sub.counters.malformed_frames += 1
        return .message_too_large
    }

    aligned_length := align_frame(header.frame_length)
    if aligned_length > sub.term.capacity {
        return .message_too_large
    }

    if term_available(&sub.term) <= u64(aligned_length) {
        return .back_pressured
    }

    write_index := term_index(&sub.term, sub.term.write_position)
    tail_available := sub.term.capacity - write_index

    if aligned_length > tail_available {
        padding_length := tail_available
        if term_available(&sub.term) <= u64(padding_length + aligned_length) {
            return .back_pressured
        }

        padding_header := Frame_Header{
            frame_length = padding_length,
            frame_type = u16(Frame_Type.PADDING),
            flags = u16(Frame_Flags.NONE),
            stream_id = sub.stream_id,
            session_id = sub.session_id,
            position = sub.term.write_position,
            reserved = 0,
        }
        write_header(sub.term.data, write_index, padding_header)
        zero_range(sub.term.data, write_index + FRAME_HEADER_LENGTH, padding_length - FRAME_HEADER_LENGTH)
        sub.term.write_position = Position(u64(sub.term.write_position) + u64(padding_length))
        sub.counters.padding_frames_written += 1
        write_index = 0
    }

    copy_bytes(sub.term.data[write_index:], frame)
    zero_range(sub.term.data, write_index + header.frame_length, aligned_length - header.frame_length)
    sub.term.write_position = Position(u64(sub.term.write_position) + u64(aligned_length))
    sub.counters.frames_written += 1
    sub.counters.bytes_written += u64(header.frame_length)
    return .none
}

write_header :: proc(buffer: []u8, offset: u32, h: Frame_Header) {
    write_u32_le(buffer, offset + 0, h.frame_length)
    write_u16_le(buffer, offset + 4, h.frame_type)
    write_u16_le(buffer, offset + 6, h.flags)
    write_u32_le(buffer, offset + 8, u32(h.stream_id))
    write_u32_le(buffer, offset + 12, u32(h.session_id))
    write_u64_le(buffer, offset + 16, u64(h.position))
    write_u64_le(buffer, offset + 24, h.reserved)
}

read_header :: proc(buffer: []u8, offset: u32) -> Frame_Header {
    return Frame_Header{
        frame_length = read_u32_le(buffer, offset + 0),
        frame_type = read_u16_le(buffer, offset + 4),
        flags = read_u16_le(buffer, offset + 6),
        stream_id = Stream_Id(read_u32_le(buffer, offset + 8)),
        session_id = Session_Id(read_u32_le(buffer, offset + 12)),
        position = Position(read_u64_le(buffer, offset + 16)),
        reserved = read_u64_le(buffer, offset + 24),
    }
}

write_u16_le :: proc(buffer: []u8, offset: u32, value: u16) {
    buffer[offset + 0] = u8(value & 0xff)
    buffer[offset + 1] = u8((value >> 8) & 0xff)
}

write_u32_le :: proc(buffer: []u8, offset: u32, value: u32) {
    buffer[offset + 0] = u8(value & 0xff)
    buffer[offset + 1] = u8((value >> 8) & 0xff)
    buffer[offset + 2] = u8((value >> 16) & 0xff)
    buffer[offset + 3] = u8((value >> 24) & 0xff)
}

write_u64_le :: proc(buffer: []u8, offset: u32, value: u64) {
    buffer[offset + 0] = u8(value & 0xff)
    buffer[offset + 1] = u8((value >> 8) & 0xff)
    buffer[offset + 2] = u8((value >> 16) & 0xff)
    buffer[offset + 3] = u8((value >> 24) & 0xff)
    buffer[offset + 4] = u8((value >> 32) & 0xff)
    buffer[offset + 5] = u8((value >> 40) & 0xff)
    buffer[offset + 6] = u8((value >> 48) & 0xff)
    buffer[offset + 7] = u8((value >> 56) & 0xff)
}

read_u16_le :: proc(buffer: []u8, offset: u32) -> u16 {
    return u16(buffer[offset + 0]) |
           (u16(buffer[offset + 1]) << 8)
}

read_u32_le :: proc(buffer: []u8, offset: u32) -> u32 {
    return u32(buffer[offset + 0]) |
           (u32(buffer[offset + 1]) << 8) |
           (u32(buffer[offset + 2]) << 16) |
           (u32(buffer[offset + 3]) << 24)
}

read_u64_le :: proc(buffer: []u8, offset: u32) -> u64 {
    return u64(buffer[offset + 0]) |
           (u64(buffer[offset + 1]) << 8) |
           (u64(buffer[offset + 2]) << 16) |
           (u64(buffer[offset + 3]) << 24) |
           (u64(buffer[offset + 4]) << 32) |
           (u64(buffer[offset + 5]) << 40) |
           (u64(buffer[offset + 6]) << 48) |
           (u64(buffer[offset + 7]) << 56)
}

copy_bytes :: proc(dst: []u8, src: []u8) {
    for i in 0..<len(src) {
        dst[i] = src[i]
    }
}

zero_range :: proc(buffer: []u8, offset: u32, length: u32) {
    for i in 0..<int(length) {
        buffer[offset + u32(i)] = 0
    }
}
