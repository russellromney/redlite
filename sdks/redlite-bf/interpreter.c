/**
 * Redlite Brainfuck Interpreter
 *
 * A custom Brainfuck interpreter with syscall extensions for Redlite.
 * We are not responsible for any psychological damage caused by maintaining this code.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../../crates/redlite-ffi/redlite.h"

#define TAPE_SIZE 30000
#define MAX_NESTED_LOOPS 1000
#define REGRET_CELL 256

// Syscall numbers
#define SYS_OPEN   1
#define SYS_CLOSE  2
#define SYS_SET    32
#define SYS_GET    33
#define SYS_DEL    34
#define SYS_EXISTS 35
#define SYS_INCR   48
#define SYS_DECR   49

typedef struct {
    unsigned char tape[TAPE_SIZE];
    int ptr;
    RedliteDb* db;
    int regret;
} BFState;

// Extract null-terminated string from tape starting at position
char* extract_string(BFState* state, int start) {
    int len = 0;
    while (state->tape[start + len] != 0 && start + len < TAPE_SIZE) {
        len++;
    }
    char* str = malloc(len + 1);
    memcpy(str, &state->tape[start], len);
    str[len] = '\0';
    return str;
}

// Execute a redlite syscall based on cell 0-7
void execute_syscall(BFState* state) {
    int syscall = state->tape[0];
    char* key;
    char* value;
    char* result;
    int64_t num_result;

    state->regret++; // Increment regret accumulator
    state->tape[REGRET_CELL] = state->regret & 0xFF;

    switch (syscall) {
        case SYS_OPEN:
            key = extract_string(state, 8); // Path in key position
            state->db = redlite_open(key);
            if (!state->db) {
                fprintf(stderr, "Failed to open database: %s\n", key);
                state->tape[0] = 0; // Error
            } else {
                state->tape[0] = 1; // Success
            }
            free(key);
            break;

        case SYS_CLOSE:
            if (state->db) {
                redlite_close(state->db);
                state->db = NULL;
            }
            state->tape[0] = 1;
            break;

        case SYS_SET:
            key = extract_string(state, 8);
            value = extract_string(state, 16);
            if (state->db) {
                redlite_set(state->db, key, value);
                state->tape[0] = 1;
            } else {
                state->tape[0] = 0;
            }
            free(key);
            free(value);
            break;

        case SYS_GET:
            key = extract_string(state, 8);
            if (state->db) {
                result = redlite_get(state->db, key);
                if (result) {
                    // Copy result to value buffer (cells 16+)
                    int len = strlen(result);
                    if (len > TAPE_SIZE - 17) len = TAPE_SIZE - 17;
                    memcpy(&state->tape[16], result, len);
                    state->tape[16 + len] = 0;
                    state->tape[0] = 1;
                    redlite_free_string(result);
                } else {
                    state->tape[16] = 0;
                    state->tape[0] = 0; // Key not found
                }
            } else {
                state->tape[0] = 0;
            }
            free(key);
            break;

        case SYS_DEL:
            key = extract_string(state, 8);
            if (state->db) {
                int deleted = redlite_del(state->db, (const char*[]){key}, 1);
                state->tape[0] = deleted > 0 ? 1 : 0;
            } else {
                state->tape[0] = 0;
            }
            free(key);
            break;

        case SYS_EXISTS:
            key = extract_string(state, 8);
            if (state->db) {
                int exists = redlite_exists(state->db, (const char*[]){key}, 1);
                state->tape[0] = exists;
            } else {
                state->tape[0] = 0;
            }
            free(key);
            break;

        case SYS_INCR:
            key = extract_string(state, 8);
            if (state->db) {
                num_result = redlite_incr(state->db, key);
                // Store result in value buffer as ASCII digits
                sprintf((char*)&state->tape[16], "%lld", num_result);
                state->tape[0] = 1;
            } else {
                state->tape[0] = 0;
            }
            free(key);
            break;

        case SYS_DECR:
            key = extract_string(state, 8);
            if (state->db) {
                num_result = redlite_decr(state->db, key);
                sprintf((char*)&state->tape[16], "%lld", num_result);
                state->tape[0] = 1;
            } else {
                state->tape[0] = 0;
            }
            free(key);
            break;

        default:
            fprintf(stderr, "Unknown syscall: %d\n", syscall);
            state->tape[0] = 0;
            break;
    }
}

int run_bf(const char* code) {
    BFState state = {0};
    state.ptr = 0;
    state.db = NULL;
    state.regret = 0;

    int code_len = strlen(code);
    int loop_stack[MAX_NESTED_LOOPS];
    int loop_depth = 0;

    for (int i = 0; i < code_len; i++) {
        switch (code[i]) {
            case '>':
                state.ptr++;
                if (state.ptr >= TAPE_SIZE) state.ptr = 0;
                break;

            case '<':
                state.ptr--;
                if (state.ptr < 0) state.ptr = TAPE_SIZE - 1;
                break;

            case '+':
                state.tape[state.ptr]++;
                break;

            case '-':
                state.tape[state.ptr]--;
                break;

            case '.':
                // Output current cell OR execute syscall if at cell 0
                if (state.ptr == 0) {
                    execute_syscall(&state);
                } else {
                    putchar(state.tape[state.ptr]);
                }
                break;

            case ',':
                state.tape[state.ptr] = getchar();
                break;

            case '[':
                if (state.tape[state.ptr] == 0) {
                    // Jump to matching ]
                    int depth = 1;
                    while (depth > 0 && i < code_len - 1) {
                        i++;
                        if (code[i] == '[') depth++;
                        if (code[i] == ']') depth--;
                    }
                } else {
                    if (loop_depth >= MAX_NESTED_LOOPS) {
                        fprintf(stderr, "Loop nesting too deep (regret level: %d)\n", state.regret);
                        return 1;
                    }
                    loop_stack[loop_depth++] = i;
                }
                break;

            case ']':
                if (state.tape[state.ptr] != 0) {
                    i = loop_stack[loop_depth - 1] - 1;
                } else {
                    loop_depth--;
                }
                break;
        }

        // Check regret overflow
        if (state.regret > 255) {
            printf("\nRegret accumulator overflow. Exiting with dignity.\n");
            break;
        }
    }

    // Cleanup
    if (state.db) {
        redlite_close(state.db);
    }

    return 0;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <file.bf>\n", argv[0]);
        fprintf(stderr, "       We're sorry you're doing this.\n");
        return 1;
    }

    FILE* f = fopen(argv[1], "r");
    if (!f) {
        fprintf(stderr, "Cannot open file: %s\n", argv[1]);
        fprintf(stderr, "       (Consider this a blessing)\n");
        return 1;
    }

    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    fseek(f, 0, SEEK_SET);

    char* code = malloc(size + 1);
    fread(code, 1, size, f);
    code[size] = '\0';
    fclose(f);

    printf("Running Brainfuck... May the odds be ever in your favor.\n");
    int result = run_bf(code);

    free(code);
    return result;
}
