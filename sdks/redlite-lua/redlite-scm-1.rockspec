rockspec_format = "3.0"
package = "redlite"
version = "scm-1"

source = {
    url = "git+https://github.com/redlite/redlite.git",
    branch = "main",
}

description = {
    summary = "Lua SDK for Redlite - Redis-compatible embedded database",
    detailed = [[
        Redlite is a Redis-compatible embedded database built in Rust with
        SQLite durability. This SDK provides LuaJIT FFI bindings for direct,
        high-performance access to Redlite from Lua.

        Features:
        - Full Redis command compatibility (strings, hashes, lists, sets, sorted sets)
        - Microsecond latency embedded operation
        - Zero configuration - no server required
        - SQLite durability with ACID transactions
        - LuaJIT FFI for maximum performance
    ]],
    homepage = "https://github.com/redlite/redlite",
    license = "MIT",
    maintainer = "Redlite Team",
}

dependencies = {
    "lua >= 5.1",
}

build = {
    type = "builtin",
    modules = {
        ["redlite"] = "redlite.lua",
        ["redlite.init"] = "redlite/init.lua",
    },
    copy_directories = {
        "spec",
    },
}

test_dependencies = {
    "busted",
}

test = {
    type = "busted",
}
