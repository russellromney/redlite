#!/usr/bin/env luajit
--[[
Oracle Test Runner for Lua SDK.

Executes YAML test specifications against the Redlite Lua SDK
and reports pass/fail results with detailed error messages.

Usage:
    luajit lua_runner.lua                    # Run all specs
    luajit lua_runner.lua spec/strings.yaml  # Run single spec
    luajit lua_runner.lua -v                 # Verbose output

Prerequisites:
    - LuaJIT (required for FFI)
    - lyaml: luarocks install lyaml
    - REDLITE_LIB_PATH environment variable or library in standard location
]]

local yaml = require("lyaml")

-- Add the Lua SDK to path
local script_dir = arg[0]:match("(.*/)")
if script_dir then
    package.path = script_dir .. "../../redlite-lua/?.lua;" .. package.path
    package.path = script_dir .. "../../redlite-lua/redlite/?.lua;" .. package.path
end

local redlite = require("redlite")

-- =============================================================================
-- Oracle Runner Class
-- =============================================================================

local OracleRunner = {}
OracleRunner.__index = OracleRunner

function OracleRunner.new(verbose)
    local self = setmetatable({}, OracleRunner)
    self.verbose = verbose or false
    self.passed = 0
    self.failed = 0
    self.errors = {}
    return self
end

function OracleRunner:run_spec_file(spec_path)
    local file = io.open(spec_path, "r")
    if not file then
        print("Error: Could not open " .. spec_path)
        return false
    end

    local content = file:read("*all")
    file:close()

    local spec = yaml.load(content)
    local spec_name = spec.name or spec_path
    local tests = spec.tests or {}

    if self.verbose then
        print(string.rep("=", 60))
        print("Running: " .. spec_name .. " (" .. #tests .. " tests)")
        print(string.rep("=", 60))
    end

    for _, test in ipairs(tests) do
        self:run_test(test, spec_name)
    end

    return #self.errors == 0
end

function OracleRunner:run_test(test, spec_name)
    local test_name = test.name or "unnamed"

    if self.verbose then
        io.write("  " .. test_name .. "... ")
        io.flush()
    end

    -- Create fresh in-memory database for each test
    local db = redlite.open_memory()

    local success, err = pcall(function()
        -- Run setup operations
        if test.setup then
            for _, op in ipairs(test.setup) do
                self:execute_cmd(db, op)
            end
        end

        -- Run test operations and check expectations
        for _, op in ipairs(test.operations) do
            local actual = self:execute_cmd(db, op)
            local expected = op.expect

            if not self:compare(actual, expected) then
                table.insert(self.errors, {
                    spec = spec_name,
                    test = test_name,
                    cmd = op.cmd,
                    args = op.args or {},
                    expected = expected,
                    actual = self:serialize(actual),
                })
                self.failed = self.failed + 1
                if self.verbose then
                    print("FAILED")
                    print("      Expected: " .. self:serialize(expected))
                    print("      Actual:   " .. self:serialize(actual))
                end
                return
            end
        end

        self.passed = self.passed + 1
        if self.verbose then
            print("PASSED")
        end
    end)

    db:close()

    if not success then
        self.failed = self.failed + 1
        table.insert(self.errors, {
            spec = spec_name,
            test = test_name,
            error = tostring(err),
        })
        if self.verbose then
            print("ERROR: " .. tostring(err))
        end
    end
end

function OracleRunner:execute_cmd(db, op)
    local cmd = string.lower(op.cmd)
    local args = op.args or {}

    -- Process args to handle special types
    args = self:process_args(args)

    -- Command dispatch table
    local handlers = {
        -- String commands
        get = function() return db:get(args[1]) end,
        set = function() return db:set(args[1], args[2], args[3]) end,
        setex = function() return db:setex(args[1], args[2], args[3]) end,
        psetex = function() return db:psetex(args[1], args[2], args[3]) end,
        getdel = function() return db:getdel(args[1]) end,
        append = function() return db:append(args[1], args[2]) end,
        strlen = function() return db:strlen(args[1]) end,
        getrange = function() return db:getrange(args[1], args[2], args[3]) end,
        setrange = function() return db:setrange(args[1], args[2], args[3]) end,
        incr = function() return db:incr(args[1]) end,
        decr = function() return db:decr(args[1]) end,
        incrby = function() return db:incrby(args[1], args[2]) end,
        decrby = function() return db:decrby(args[1], args[2]) end,
        incrbyfloat = function() return db:incrbyfloat(args[1], args[2]) end,
        mget = function()
            if type(args[1]) == "table" then
                return db:mget(unpack(args[1]))
            end
            return db:mget(unpack(args))
        end,
        mset = function()
            local mapping = {}
            if type(args[1]) == "table" then
                for _, pair in ipairs(args) do
                    if type(pair) == "table" and #pair == 2 then
                        mapping[pair[1]] = pair[2]
                    end
                end
            end
            return db:mset(mapping)
        end,

        -- Key commands
        del = function()
            if type(args[1]) == "table" then
                return db:del(unpack(args[1]))
            end
            return db:del(unpack(args))
        end,
        exists = function()
            if type(args[1]) == "table" then
                return db:exists(unpack(args[1]))
            end
            return db:exists(unpack(args))
        end,
        type = function() return db:type(args[1]) end,
        ttl = function() return db:ttl(args[1]) end,
        pttl = function() return db:pttl(args[1]) end,
        expire = function() return db:expire(args[1], args[2]) end,
        pexpire = function() return db:pexpire(args[1], args[2]) end,
        expireat = function() return db:expireat(args[1], args[2]) end,
        pexpireat = function() return db:pexpireat(args[1], args[2]) end,
        persist = function() return db:persist(args[1]) end,
        rename = function() return db:rename(args[1], args[2]) end,
        renamenx = function() return db:renamenx(args[1], args[2]) end,
        keys = function() return db:keys(args[1]) end,
        dbsize = function() return db:dbsize() end,
        flushdb = function() return db:flushdb() end,

        -- Hash commands
        hset = function()
            if #args == 3 then
                return db:hset(args[1], args[2], args[3])
            end
            return db:hset(args[1], args[2])
        end,
        hget = function() return db:hget(args[1], args[2]) end,
        hdel = function()
            if type(args[2]) == "table" then
                return db:hdel(args[1], unpack(args[2]))
            end
            return db:hdel(args[1], select(2, unpack(args)))
        end,
        hexists = function() return db:hexists(args[1], args[2]) end,
        hlen = function() return db:hlen(args[1]) end,
        hkeys = function() return db:hkeys(args[1]) end,
        hvals = function() return db:hvals(args[1]) end,
        hincrby = function() return db:hincrby(args[1], args[2], args[3]) end,
        hgetall = function() return db:hgetall(args[1]) end,
        hmget = function()
            if type(args[2]) == "table" then
                return db:hmget(args[1], unpack(args[2]))
            end
            return db:hmget(args[1], select(2, unpack(args)))
        end,

        -- List commands
        lpush = function()
            if type(args[2]) == "table" then
                return db:lpush(args[1], unpack(args[2]))
            end
            return db:lpush(args[1], select(2, unpack(args)))
        end,
        rpush = function()
            if type(args[2]) == "table" then
                return db:rpush(args[1], unpack(args[2]))
            end
            return db:rpush(args[1], select(2, unpack(args)))
        end,
        lpop = function() return db:lpop(args[1], args[2] or 1) end,
        rpop = function() return db:rpop(args[1], args[2] or 1) end,
        llen = function() return db:llen(args[1]) end,
        lrange = function() return db:lrange(args[1], args[2], args[3]) end,
        lindex = function() return db:lindex(args[1], args[2]) end,

        -- Set commands
        sadd = function()
            if type(args[2]) == "table" then
                return db:sadd(args[1], unpack(args[2]))
            end
            return db:sadd(args[1], select(2, unpack(args)))
        end,
        srem = function()
            if type(args[2]) == "table" then
                return db:srem(args[1], unpack(args[2]))
            end
            return db:srem(args[1], select(2, unpack(args)))
        end,
        smembers = function() return db:smembers(args[1]) end,
        sismember = function() return db:sismember(args[1], args[2]) end,
        scard = function() return db:scard(args[1]) end,

        -- Sorted set commands
        zadd = function()
            if type(args[2]) == "table" then
                -- [[score, member], ...]
                local members = {}
                for _, pair in ipairs(args[2]) do
                    if type(pair) == "table" and #pair == 2 then
                        table.insert(members, {pair[1], pair[2]})
                    end
                end
                return db:zadd(args[1], members)
            end
            return db:zadd(args[1], args[2], args[3])
        end,
        zrem = function()
            if type(args[2]) == "table" then
                return db:zrem(args[1], unpack(args[2]))
            end
            return db:zrem(args[1], select(2, unpack(args)))
        end,
        zscore = function() return db:zscore(args[1], args[2]) end,
        zcard = function() return db:zcard(args[1]) end,
        zcount = function() return db:zcount(args[1], args[2], args[3]) end,
        zincrby = function() return db:zincrby(args[1], args[2], args[3]) end,
        zrange = function() return db:zrange(args[1], args[2], args[3], args[4]) end,
        zrevrange = function() return db:zrevrange(args[1], args[2], args[3], args[4]) end,
    }

    local handler = handlers[cmd]
    if not handler then
        error("Unknown command: " .. cmd)
    end

    return handler()
end

function OracleRunner:process_args(args)
    local result = {}
    for i, arg in ipairs(args) do
        if type(arg) == "table" and arg.bytes then
            -- Handle bytes type
            result[i] = string.char(unpack(arg.bytes))
        elseif type(arg) == "table" then
            result[i] = self:process_args(arg)
        else
            result[i] = arg
        end
    end
    return result
end

function OracleRunner:compare(actual, expected)
    if expected == nil then
        return actual == nil
    end

    if type(expected) == "table" then
        return self:compare_special(actual, expected)
    end

    if type(expected) == "boolean" then
        return actual == expected
    end

    if type(expected) == "number" then
        if type(actual) == "number" then
            if math.floor(expected) == expected then
                -- Integer comparison
                return actual == expected
            else
                -- Float comparison with tolerance
                return math.abs(actual - expected) < 0.001
            end
        end
        return false
    end

    if type(expected) == "string" then
        return tostring(actual) == expected
    end

    return actual == expected
end

function OracleRunner:compare_special(actual, expected)
    if expected.bytes then
        local exp_str = string.char(unpack(expected.bytes))
        return actual == exp_str
    end

    if expected.set then
        -- Unordered set comparison
        if type(actual) ~= "table" then return false end
        local actual_set = {}
        for _, v in ipairs(actual) do
            actual_set[tostring(v)] = true
        end
        local exp_set = {}
        for _, v in ipairs(expected.set) do
            exp_set[tostring(v)] = true
        end
        for k in pairs(exp_set) do
            if not actual_set[k] then return false end
        end
        for k in pairs(actual_set) do
            if not exp_set[k] then return false end
        end
        return true
    end

    if expected.dict then
        if type(actual) ~= "table" then return false end
        for k, v in pairs(expected.dict) do
            if tostring(actual[k]) ~= tostring(v) then return false end
        end
        for k in pairs(actual) do
            if expected.dict[k] == nil then return false end
        end
        return true
    end

    if expected.range then
        local low, high = expected.range[1], expected.range[2]
        return actual >= low and actual <= high
    end

    if expected.approx then
        local target = expected.approx
        local tol = expected.tol or 0.001
        return math.abs(actual - target) <= tol
    end

    if expected["type"] then
        local type_map = {
            bytes = "string",
            str = "string",
            int = "number",
            float = "number",
            list = "table",
            dict = "table",
            set = "table",
        }
        return type(actual) == type_map[expected["type"]]
    end

    if expected.contains then
        return tostring(actual):find(expected.contains, 1, true) ~= nil
    end

    -- Array comparison
    if #expected > 0 or next(expected) == nil then
        if type(actual) ~= "table" then return false end
        if #actual ~= #expected then return false end
        for i, v in ipairs(expected) do
            if not self:compare(actual[i], v) then return false end
        end
        return true
    end

    return false
end

function OracleRunner:serialize(value)
    if value == nil then
        return "nil"
    end

    if type(value) == "string" then
        return '"' .. value .. '"'
    end

    if type(value) == "table" then
        local parts = {}
        local is_array = #value > 0
        if is_array then
            for _, v in ipairs(value) do
                table.insert(parts, self:serialize(v))
            end
            return "[" .. table.concat(parts, ", ") .. "]"
        else
            for k, v in pairs(value) do
                table.insert(parts, tostring(k) .. "=" .. self:serialize(v))
            end
            return "{" .. table.concat(parts, ", ") .. "}"
        end
    end

    return tostring(value)
end

function OracleRunner:summary()
    local total = self.passed + self.failed
    return string.format("%d/%d passed, %d failed", self.passed, total, self.failed)
end

-- =============================================================================
-- Main
-- =============================================================================

local function main()
    local verbose = false
    local specs = {}

    -- Parse arguments
    for i = 1, #arg do
        if arg[i] == "-v" or arg[i] == "--verbose" then
            verbose = true
        else
            table.insert(specs, arg[i])
        end
    end

    -- Find spec directory
    local spec_dir = script_dir and (script_dir .. "../spec/") or "../spec/"

    -- If no specs specified, run all
    if #specs == 0 then
        local handle = io.popen("ls " .. spec_dir .. "*.yaml 2>/dev/null")
        if handle then
            for line in handle:lines() do
                table.insert(specs, line)
            end
            handle:close()
        end
    end

    local runner = OracleRunner.new(verbose)

    for _, spec_file in ipairs(specs) do
        runner:run_spec_file(spec_file)
    end

    -- Print summary
    print(string.rep("=", 60))
    print("Oracle Test Results: " .. runner:summary())
    print(string.rep("=", 60))

    if #runner.errors > 0 then
        print("\nFailures:")
        for _, err in ipairs(runner.errors) do
            if err.error then
                print(string.format("  - %s / %s: %s", err.spec, err.test, err.error))
            else
                print(string.format("  - %s / %s / %s", err.spec, err.test, err.cmd))
                print("      Expected: " .. runner:serialize(err.expected))
                print("      Actual:   " .. runner:serialize(err.actual))
            end
        end
        os.exit(1)
    end

    os.exit(0)
end

main()
