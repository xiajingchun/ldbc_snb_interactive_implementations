package com.ldbc.impls.workloads.ldbc.snb.nebula;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.impls.workloads.ldbc.snb.QueryStore;
import com.ldbc.impls.workloads.ldbc.snb.converter.Converter;
import com.ldbc.impls.workloads.ldbc.snb.nebula.converter.NebulaConverter;

public class NebulaQueryStore extends QueryStore {
    public NebulaQueryStore(String path) throws DbException  {
        super(path, ".ngql");
    }

    @Override
    public Converter getConverter() {
        return new NebulaConverter();
    }

    @Override
    public String getShortQuery1PersonProfile(LdbcShortQuery1PersonProfile operation) {
        return prepare(
                QueryType.InteractiveShortQuery1,
                ImmutableMap.of(LdbcShortQuery1PersonProfile.PERSON_ID, "person-" + getConverter().convertId(operation.personId()))
        );
    }
}
