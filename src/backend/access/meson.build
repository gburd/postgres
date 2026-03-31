# Copyright (c) 2022-2026, PostgreSQL Global Development Group

subdir('brin')
subdir('common')
subdir('gin')
subdir('gist')
subdir('hash')
subdir('heap')

# Noxu table AM (optional, enabled by default)
if not get_option('noxu').disabled()
  subdir('noxu')
endif

subdir('index')
subdir('nbtree')
subdir('rmgrdesc')
subdir('sequence')
subdir('spgist')
subdir('table')
subdir('tablesample')
subdir('transam')
subdir('undo')
