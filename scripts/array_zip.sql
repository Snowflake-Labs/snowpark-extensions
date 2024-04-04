create or replace function array_zip(ARRAY1 array, ARRAY2 array) returns array
language javascript as
$$
var arrays = [ARRAY1, ARRAY2];
return arrays[0].map((_, index) => arrays.map(array => array[index]));
$$;

create or replace function array_zip(ARRAY1 array, ARRAY2 array,ARRAY3 array) returns array
language javascript as
$$
var arrays = [ARRAY1, ARRAY2, ARRAY3];
return arrays[0].map((_, index) => arrays.map(array => array[index]));
$$;

create or replace function array_zip(ARRAY1 array, ARRAY2 array,ARRAY3 array, ARRAY4 array) returns array
language javascript as
$$
var arrays = [ARRAY1, ARRAY2, ARRAY3, ARRAY4];
return arrays[0].map((_, index) => arrays.map(array => array[index]));
$$;

create or replace function array_zip(ARRAY1 array, ARRAY2 array,ARRAY3 array, ARRAY4 array, ARRAY5 array) returns array
language javascript as
$$
var arrays = [ARRAY1, ARRAY2, ARRAY3, ARRAY4, ARRAY5];
return arrays[0].map((_, index) => arrays.map(array => array[index]));
$$;
