// Angry tests for json_escape (util.c).
//
// dat.h promises: "JSON-escape src into dst. Writes at most dst_size-1
// chars + NUL. Truncation-safe: never writes past dst_size, always
// NUL-terminates when dst_size > 0. Returns number of chars written
// (excluding NUL)."
//
// Every way this can go wrong is silent at the point of failure and
// loud hours later in someone else's parser: a half-written escape at
// the truncation point turns the enclosing log record into garbage, a
// control byte that slips past the switch makes the record illegal
// JSON, and a return value that disagrees with what is in the buffer
// makes every caller's length arithmetic wrong. The escape FORM is not
// promised (a two-char escape and a \u00xx escape are both legal),
// so these tests pin the
// decoded round trip, not the spelling.

#include "dat.h"
#include "ct/ct.h"
#include <stdio.h>
#include <string.h>

// A reduced JSON string-body decoder. It is the reference the contract
// is stated against: whatever escape form json_escape picks, the body
// it produces must be a legal JSON string body and must decode back to
// the bytes it was handed. Returns the decoded length, or -1 when the
// text is not a legal JSON string body (raw control byte, unescaped
// quote, unknown escape, dangling backslash, short \u).
static int
je_decode(const char *s, char *out, size_t outlen)
{
    size_t o = 0;
    size_t i = 0;

    while (s[i]) {
        unsigned char c = (unsigned char)s[i++];
        if (c < 0x20 || c == '"') return -1;
        if (o + 1 >= outlen) return -1;
        if (c != '\\') {
            out[o++] = (char)c;
            continue;
        }
        char e = s[i++];
        switch (e) {
        case '"':  out[o++] = '"';  break;
        case '\\': out[o++] = '\\'; break;
        case '/':  out[o++] = '/';  break;
        case 'b':  out[o++] = '\b'; break;
        case 'f':  out[o++] = '\f'; break;
        case 'n':  out[o++] = '\n'; break;
        case 'r':  out[o++] = '\r'; break;
        case 't':  out[o++] = '\t'; break;
        case 'u': {
            unsigned v = 0;
            for (int k = 0; k < 4; k++) {
                char h = s[i + (size_t)k];
                int d;
                if (h >= '0' && h <= '9')      d = h - '0';
                else if (h >= 'a' && h <= 'f') d = h - 'a' + 10;
                else if (h >= 'A' && h <= 'F') d = h - 'A' + 10;
                else return -1;
                v = v * 16 + (unsigned)d;
            }
            i += 4;
            if (v > 0xff) return -1;
            out[o++] = (char)v;
            break;
        }
        default:
            return -1;
        }
    }
    out[o] = 0;
    return (int)o;
}


// One representative per escape class the switch owns, plus a
// pass-through byte. The expected text is the form the code currently
// emits; the tests that use this table are about WHOLENESS and length,
// which every legal form has to satisfy.
struct je_class {
    const char *src;
    const char *want;
};

static const struct je_class je_classes[] = {
    { "\"",   "\\\""    },
    { "\\",   "\\\\"    },
    { "\b",   "\\b"     },
    { "\f",   "\\f"     },
    { "\n",   "\\n"     },
    { "\r",   "\\r"     },
    { "\t",   "\\t"     },
    { "\x01", "\\u0001" },
    { "\x1f", "\\u001f" },
    { "A",    "A"       },
    { NULL,   NULL      },
};


static const char *
je_first_class_not_written_whole(void)
{
    for (int i = 0; je_classes[i].src; i++) {
        const char *want = je_classes[i].want;
        size_t need = strlen(want);
        char buf[32];

        memset(buf, 'X', sizeof buf);
        size_t n = json_escape(buf, need + 1, je_classes[i].src);
        if (n != need) return want;
        if (memcmp(buf, want, need + 1) != 0) return want;
        if (buf[need + 1] != 'X') return want;
    }
    return NULL;
}


static const char *
je_first_class_started_one_byte_short(void)
{
    for (int i = 0; je_classes[i].src; i++) {
        const char *want = je_classes[i].want;
        size_t need = strlen(want);
        char buf[32];

        memset(buf, 'X', sizeof buf);
        size_t n = json_escape(buf, need, je_classes[i].src);
        if (n != 0) return want;
        if (buf[0] != 0) return want;
        if (buf[need] != 'X') return want;
    }
    return NULL;
}


static int
je_first_size_whose_return_lies(const char *src)
{
    for (size_t k = 1; k <= 48; k++) {
        char buf[64];

        memset(buf, 'X', sizeof buf);
        size_t n = json_escape(buf, k, src);
        if (n >= k) return (int)k;
        if (n != strlen(buf)) return (int)k;
        if (buf[k] != 'X') return (int)k;
    }
    return -1;
}


static int
je_first_size_that_leaves_illegal_json(const char *src)
{
    for (size_t k = 1; k <= 48; k++) {
        char buf[64];
        char dec[64];

        memset(buf, 'X', sizeof buf);
        json_escape(buf, k, src);
        if (je_decode(buf, dec, sizeof dec) < 0) return (int)k;
    }
    return -1;
}


static int
je_first_byte_escaped_wrongly(void)
{
    for (int c = 1; c <= 255; c++) {
        char src[2];
        char esc[16];
        char dec[16];

        src[0] = (char)c;
        src[1] = 0;
        size_t n = json_escape(esc, sizeof esc, src);
        if (n == 0) return c;
        for (size_t i = 0; i < n; i++) {
            if ((unsigned char)esc[i] < 0x20) return c;
        }
        if (je_decode(esc, dec, sizeof dec) != 1) return c;
        if ((unsigned char)dec[0] != (unsigned char)c) return c;
    }
    return -1;
}


// Exact fit: dst_size is escape length + 1, the smallest buffer that
// can hold the whole unit and its NUL. A guard one byte too eager here
// silently drops the last character of every message that happens to
// end at the buffer edge.
void
cttest_json_escape_writes_every_escape_class_whole_when_the_buffer_fits_exactly(void)
{
    const char *bad = je_first_class_not_written_whole();

    assertf(bad == NULL,
            "the escape [%s] must be written whole into a buffer sized "
            "exactly for it plus its NUL",
            bad ? bad : "");
}


// One byte below the exact fit. The unit cannot be written, so none of
// it may be written: half of an escape is a backslash with nothing
// after it, and that poisons the whole enclosing record.
void
cttest_json_escape_writes_none_of_an_escape_that_is_one_byte_short(void)
{
    const char *bad = je_first_class_started_one_byte_short();

    assertf(bad == NULL,
            "the escape [%s] must not be started at all when the buffer "
            "is one byte too small for it",
            bad ? bad : "");
}


// The return value is the only thing a caller can use to append to the
// buffer. It must be exactly what is in it, at every truncation point.
void
cttest_json_escape_returns_the_number_of_bytes_it_left_in_the_buffer(void)
{
    const char *src = "a\"b\\c\nd\x01_e";

    int bad = je_first_size_whose_return_lies(src);

    assertf(bad < 0,
            "for src [%s] at dst_size %d the return value must equal "
            "strlen(dst) and stay below dst_size",
            src, bad);
}


// The property the truncation branches exist for: whatever the buffer
// size, what comes out is still a legal JSON string body. This is the
// assertion that a partial memcpy of an escape cannot survive.
void
cttest_json_escape_leaves_a_legal_json_string_at_every_truncation_point(void)
{
    const char *src = "x\"\\\n\x01\x1f\ty";

    int bad = je_first_size_that_leaves_illegal_json(src);

    assertf(bad < 0,
            "the output must still decode as a JSON string body at "
            "dst_size %d",
            bad);
}


// The round trip, with one representative of every class in one string:
// quote, backslash, a two-char escape, two \\u-form bytes, a UTF-8
// pair, a lone high byte and 0x7f.
void
cttest_json_escape_decodes_back_to_the_bytes_it_was_given(void)
{
    char src[] = { 'a', '"', 'b', '\\', 'c', '\n', 0x01, 0x1f,
                   (char)0xc3, (char)0xa9, (char)0xff, 0x7f, 'z', 0 };
    char esc[128];
    char dec[128];

    size_t n = json_escape(esc, sizeof esc, src);
    int d = je_decode(esc, dec, sizeof dec);

    assertf(d == (int)(sizeof src - 1) && memcmp(dec, src, sizeof src) == 0,
            "the %zu escaped bytes must decode back to the %zu input "
            "bytes, got %d decoded",
            n, sizeof src - 1, d);
}


// Every byte a C string can carry. Below 0x20 nothing may reach the
// output raw; at and above it the byte must survive unchanged, which
// includes 0x7f and every high byte a UTF-8 message is made of.
void
cttest_json_escape_leaves_no_raw_control_byte_for_any_input_byte(void)
{
    int bad = je_first_byte_escaped_wrongly();

    assertf(bad < 0,
            "byte 0x%02x must come out either escaped or unchanged, with "
            "no raw byte below 0x20 in the output",
            bad < 0 ? 0 : bad);
}
