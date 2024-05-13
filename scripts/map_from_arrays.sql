create or replace function map_from_arrays(KEYS array, "VALUES" ARRAY)
returns object
language javascript
as
$$
  if (KEYS.length !== VALUES.length) {
    throw new Error("Arrays must have the same length");
  }
  
  const resultMap = {};
  
  for (let i = 0; i < KEYS.length; i++) {
    resultMap[KEYS[i]] = VALUES[i];
  }
  
  return resultMap;
$$;

-- select map_from_arrays(array_construct(1,2),array_construct('a','b'))
