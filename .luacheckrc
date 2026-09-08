std = 'luajit'
globals = {'box', '_TARANTOOL', 'tonumber64', 'utf8', 'table'}
ignore = {
    -- Unused argument <self>.
    '212/self',
    -- Shadowing a local variable.
    '421',
    -- Shadowing an upvalue.
    '431',
    -- Shadowing an upvalue argument.
    '432',
}

include_files = {
    'pregel/**/*.lua',
    'test/**/*_test.lua',
    'test/apps/*.lua',
    'test/helpers/*.lua',
    'test/instances/*.lua',
    'examples/**/*.lua',
    'tools/*.lua',
}

exclude_files = {
    'test/var/*',
    '.rocks/*',
}
