create or replace function public.size(data variant) returns integer
language sql
CALLED ON NULL INPUT
as $$
case 
 when data is null then  -1
 when typeof(data) = 'OBJECT' then array_size(OBJECT_KEYS(data::OBJECT))
 when typeof(data) = 'ARRAY' then array_size(data)
end 
$$;

create or replace function public.size(data array) returns integer
language sql 
CALLED ON NULL INPUT
as $$
case when data is null then -1
else array_size(data)
end
$$;

create or replace function public.size(data object) returns integer
language sql 
CALLED ON NULL INPUT
as $$
case when data is null then -1
else array_size(object_keys(data))
end
$$;
