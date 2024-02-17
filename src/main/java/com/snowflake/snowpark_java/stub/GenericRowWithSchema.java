package com.snowflake.snowpark_java.stub;

import java.util.Iterator;

import com.snowflake.snowpark_java.Row;
import com.snowflake.snowpark_java.types.StructType;

public class GenericRowWithSchema extends Row implements Iterator<Row> {

    public GenericRowWithSchema(Object[] array, StructType schema)  {
        super(null);
    }

    @Override
    public boolean hasNext() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'hasNext'");
    }

    @Override
    public Row next() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'next'");
    }

}
