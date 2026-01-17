--[[
Redlite Lua SDK - Module Entry Point

This file enables both:
  require("redlite")      -- loads redlite/init.lua
  require("redlite.lua")  -- loads the FFI wrapper directly

Copyright (c) 2024 Redlite
MIT License
]]

-- Load the FFI wrapper from the parent directory
local current_dir = debug.getinfo(1, "S").source:match("^@?(.*/)") or ""
package.path = current_dir .. "../?.lua;" .. package.path

return require("redlite")
