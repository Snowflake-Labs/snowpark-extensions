create or replace function public.substring_index(str string, delim string, index number) returns string
language sql as
$$
case 
when (index < 0) THEN substring(str,REGEXP_INSTR(reverse(str),delim,1,abs(index),1))
else substring(str,REGEXP_INSTR(str,delim,1,index,1))
end
$$