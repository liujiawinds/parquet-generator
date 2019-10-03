package com.hansight.commons.parquet;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by liujia on 2019/10/3.
 */
public class SchemaTest {

    @Test
    public void testUnionSchema() {
        Schema unionType = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));
        Assert.assertEquals("[\"null\",\"string\"]", unionType.toString());
    }
}
