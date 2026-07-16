#include "tinytest.h"
#include "uuid.h"
#include <string.h>
#include <stdint.h>

suite("uuid library") {
  group("v4 generation") {
    it("generates a valid v4 uuid") {
      uuid_state_t state;
      uuid_seed(&state);
      uuid_t out;
      uuid4_gen(&state, &out);
      
      // check version 4 (bits 4-7 of bytes[6] should be 0x4)
      check((out.bytes[6] & 0xf0) == 0x40);
      // check variant (bits 6-7 of bytes[8] should be 0x8, 0x9, 0xa, or 0xb -> 10xx)
      check((out.bytes[8] & 0xc0) == 0x80);
    }
  }

  group("v7 generation") {
    it("generates a valid v7 uuid") {
      uuid_state_t state;
      uuid_seed(&state);
      uuid_t out;
      uuid7_gen(&state, &out);
      
      // check version 7 (bits 4-7 of bytes[6] should be 0x7)
      check((out.bytes[6] & 0xf0) == 0x70);
      // check variant
      check((out.bytes[8] & 0xc0) == 0x80);
      
      // check timestamp is non-zero (assuming time is not 1970)
      uint64_t ts = 0;
      for (int i = 0; i < 6; i++) {
          ts = (ts << 8) | out.bytes[i];
      }
      check(ts > 0);
    }
  }

  group("string conversion") {
    it("converts to and from string correctly") {
      uuid_state_t state;
      uuid_seed(&state);
      uuid_t original, decoded;
      char buffer[UUID4_STR_BUFFER_SIZE];
      
      uuid4_gen(&state, &original);
      check(uuid_to_s(original, buffer, sizeof(buffer)));
      
      check(uuid_from_s(buffer, &decoded));
      check(memcmp(original.bytes, decoded.bytes, 16) == 0);
    }
    
    it("parses and validates a v7 uuid string correctly") {
      const char* v7_str = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
      uuid_t decoded;
      char encoded[UUID4_STR_BUFFER_SIZE];
      
      check(uuid_from_s(v7_str, &decoded));
      // Version should be 7
      check((decoded.bytes[6] & 0xf0) == 0x70);
      
      // Round trip check
      check(uuid_to_s(decoded, encoded, sizeof(encoded)));
      check(strcmp(v7_str, encoded) == 0);
    }
    
    it("fails on invalid string formats") {
        uuid_t out;
        check(uuid_from_s("550e8400-e29b-41d4-a716-446655440000", &out)); // valid example for ref
        check(!uuid_from_s("invalid-uuid-format", &out));
        // non-hex characters
        check(!uuid_from_s("zzzzzzzz-zzzz-4xxx-yxxx-xxxxxxxxxxxx", &out));
        // too short
        check(!uuid_from_s("550e8400-e29b-41d4-a716-44665544000", &out));
        // too long
        check(!uuid_from_s("550e8400-e29b-41d4-a716-4466554400000", &out));
    }
  }
}
