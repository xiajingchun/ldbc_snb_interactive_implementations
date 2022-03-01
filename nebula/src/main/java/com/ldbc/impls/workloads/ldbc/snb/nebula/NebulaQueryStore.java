package com.ldbc.impls.workloads.ldbc.snb.nebula;

import com.google.common.collect.ImmutableMap;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
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
    public String getQuery1(LdbcQuery1 operation) {
        return prepare(QueryType.InteractiveComplexQuery1, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .put("firstName", getConverter().convertString(operation.firstName()))
                .build());
    }

    @Override
    public String getQuery3(LdbcQuery3 operation) {
        return prepare(QueryType.InteractiveComplexQuery3, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .put("countryXName", getConverter().convertString(operation.countryXName()))
                .put("countryYName", getConverter().convertString(operation.countryYName()))
                .put("startDate", getConverter().convertDateTime(operation.startDate()))  // TODO:
                .put("durationDays", getConverter().convertInteger(operation.durationDays()))  // TODO:
                .build());
    }

    @Override
    public String getQuery4(LdbcQuery4 operation) {
        return prepare(QueryType.InteractiveComplexQuery4, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .put("startDate", getConverter().convertDateTime(operation.startDate()))  // TODO:
                .put("durationDays", getConverter().convertInteger(operation.durationDays()))  // TODO:
                .build());
    }

    public String getQuery5(LdbcQuery5 operation) {
        return prepare(QueryType.InteractiveComplexQuery5, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .put("minDate", getConverter().convertDateTime(operation.minDate()))  // TODO:
                .build());
    }

    public String getQuery6(LdbcQuery6 operation) {
        return prepare(QueryType.InteractiveComplexQuery6, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .put("tagName", getConverter().convertString(operation.tagName()))
                .build());
    }

    public String getQuery7(LdbcQuery7 operation) {
        return prepare(QueryType.InteractiveComplexQuery7, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .build());
    }


    public String getQuery8(LdbcQuery8 operation) {
        return prepare(QueryType.InteractiveComplexQuery8, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .build());
    }

    public String getQuery9(LdbcQuery9 operation) {
        return prepare(QueryType.InteractiveComplexQuery9, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .put("maxDate", getConverter().convertDateTime(operation.maxDate()))  // TODO:
                .build());
    }

    public String getQuery10(LdbcQuery10 operation) {
        return prepare(QueryType.InteractiveComplexQuery10, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .put("month", getConverter().convertInteger(operation.month()))  // TODO:
                .put("nextMonth", getConverter().convertInteger(operation.month() % 12 + 1))  // TODO:
                .build());
    }

    public String getQuery11(LdbcQuery11 operation) {
        return prepare(QueryType.InteractiveComplexQuery11, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .put("countryName", getConverter().convertString(operation.countryName()))
                .put("workFromYear", getConverter().convertInteger(operation.workFromYear()))
                .build());
    }

    public String getQuery12(LdbcQuery12 operation) {
        return prepare(QueryType.InteractiveComplexQuery12, new ImmutableMap.Builder<String, String>()
                .put("personId", getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
                .put("tagClassName", getConverter().convertString(operation.tagClassName()))
                .build());
    }

    public String getQuery13(LdbcQuery13 operation) {
        return prepare(QueryType.InteractiveComplexQuery13, new ImmutableMap.Builder<String, String>()
                .put("person1Id", getConverter().convertString("person-" + getConverter().convertId(operation.person1Id())))
                .put("person2Id", getConverter().convertString("person-" + getConverter().convertId(operation.person2Id())))
                .build());
    }

    public String getQuery14(LdbcQuery14 operation) {
        return prepare(QueryType.InteractiveComplexQuery14, new ImmutableMap.Builder<String, String>()
                .put("person1Id", getConverter().convertString("person-" + getConverter().convertId(operation.person1Id())))
                .put("person2Id", getConverter().convertString("person-" + getConverter().convertId(operation.person2Id())))
                .build());
    }

    // Interactive short reads

    @Override
    public String getShortQuery1PersonProfile(LdbcShortQuery1PersonProfile operation) {
        return prepare(
                QueryType.InteractiveShortQuery1,
                ImmutableMap.of(
                        LdbcShortQuery1PersonProfile.PERSON_ID, getConverter().convertString("person-" + getConverter().convertId(operation.personId())))
        );
    }
}
