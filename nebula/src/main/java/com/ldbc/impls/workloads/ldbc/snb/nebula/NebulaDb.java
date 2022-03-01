package com.ldbc.impls.workloads.ldbc.snb.nebula;

import com.ldbc.driver.DbException;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.impls.workloads.ldbc.snb.db.BaseDb;
import com.ldbc.impls.workloads.ldbc.snb.nebula.converter.NebulaConverter;
import com.ldbc.impls.workloads.ldbc.snb.nebula.operationhandlers.NebulaListOperationHandler;
import com.ldbc.impls.workloads.ldbc.snb.nebula.operationhandlers.NebulaSingletonOperationHandler;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import com.vesoft.nebula.client.graph.exception.InvalidValueException;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NebulaDb  extends BaseDb<NebulaQueryStore> {
    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        try {
            dcs = new NebulaDbConnectionState(properties, new NebulaQueryStore(properties.get("queryDir")));
        } catch (UnknownHostException e) {
            throw new DbException(e.getMessage());
        }
    }

    // Interactive complex reads

    public static class InteractiveQuery1 extends NebulaListOperationHandler<LdbcQuery1, LdbcQuery1Result> {

        @Override
        public String getQueryString(NebulaDbConnectionState state, LdbcQuery1 operation) {
            return state.getQueryStore().getQuery1(operation);
        }

        @Override
        public LdbcQuery1Result convertSingleResult(ResultSet.Record record) throws UnsupportedEncodingException {

            List<String> emails = new ArrayList<>();
            if (!record.get(8).isNull()) {
                for (ValueWrapper email : record.get(8).asList()) {
                    emails.add(email.asString());
                }
            }

            List<String> languages = new ArrayList<>();
            if (!record.get(9).isNull()) {
                for (ValueWrapper lang : record.get(9).asList()) {
                    languages.add(lang.asString());
                }
            }

            List<List<Object>> universities = new ArrayList<>();
            if (!record.get(11).isNull()) {
                // TODO:
            }

            List<List<Object>> companies = new ArrayList<>();
            if (!record.get(12).isNull()) {
                // TODO:
            }

            long friendId = record.get(0).asLong();
            String friendLastName = record.get(1).asString();
            int distanceFromPerson = (int) record.get(2).asLong();
            long friendBirthday = 0;
            // TODO: long friendBirthday = NebulaConverter.convertLongDateToEpoch(record.get(3).asLong());
            long friendCreationDate = 0;
            // TODO: long friendCreationDate = NebulaConverter.convertLongTimestampToEpoch(record.get(4).asLong());
            String friendGender = record.get(5).asString();
            String friendBrowserUsed = record.get(6).asString();
            String friendLocationIp = record.get(7).asString();
            String friendCityName = record.get(10).asString();
            return new LdbcQuery1Result(
                    friendId,
                    friendLastName,
                    distanceFromPerson,
                    friendBirthday,
                    friendCreationDate,
                    friendGender,
                    friendBrowserUsed,
                    friendLocationIp,
                    emails,
                    languages,
                    friendCityName,
                    universities,
                    companies);
        }
    }

    // Interactive short reads

    public static class ShortQuery1PersonProfile extends NebulaSingletonOperationHandler<LdbcShortQuery1PersonProfile, LdbcShortQuery1PersonProfileResult> {

        @Override
        public String getQueryString(NebulaDbConnectionState state, LdbcShortQuery1PersonProfile operation) {
            return state.getQueryStore().getShortQuery1PersonProfile(operation);
        }

        @Override
        public LdbcShortQuery1PersonProfileResult convertSingleResult(ResultSet.Record record) throws UnsupportedEncodingException {
            String firstName = record.get(0).asString();
            String lastName = record.get(1).asString();
            long birthday = 0;
            // TODO: long birthday = NebulaConverter.convertLongDateToEpoch(record.get(2).asLong());
            String locationIP = record.get(3).asString();
            String browserUsed = record.get(4).asString();
            long cityId = record.get(5).asLong();
            String gender = record.get(6).asString();
            long creationDate = 0;
            // TODO: long creationDate = NebulaConverter.convertLongTimestampToEpoch(record.get(7).asLong());
            return new LdbcShortQuery1PersonProfileResult(
                    firstName,
                    lastName,
                    birthday,
                    locationIP,
                    browserUsed,
                    cityId,
                    gender,
                    creationDate);
        }
    }
}
