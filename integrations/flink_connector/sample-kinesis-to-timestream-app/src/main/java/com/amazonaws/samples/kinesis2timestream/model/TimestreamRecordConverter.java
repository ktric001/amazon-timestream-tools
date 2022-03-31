package com.amazonaws.samples.kinesis2timestream.model;

import java.util.List;

import software.amazon.awssdk.services.timestreamwrite.model.Dimension;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValue;
import software.amazon.awssdk.services.timestreamwrite.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.TimeUnit;

public class TimestreamRecordConverter {
    public static Record convert(final MyHostBase customObject) {
        if (customObject.getClass().equals(MyHostMetric.class)) {
            return convertFromMetric((MyHostMetric) customObject);
        } else if (customObject.getClass().equals(MyHostEvent.class)) {
            return convertFromEvent((MyHostEvent) customObject);
        } else {
            throw new RuntimeException("Invalid object type: " + customObject.getClass().getSimpleName());
        }
    }

    private static Record convertFromEvent(final MyHostEvent event) {
        List<Dimension> dimensions = List.of(
                Dimension.builder()
                        .name("region")
                        .value(event.getRegion()).build(),
                Dimension.builder()
                        .name("cell")
                        .value(event.getCell()).build(),
                Dimension.builder()
                        .name("silo")
                        .value(event.getSilo()).build(),
                Dimension.builder()
                        .name("availability_zone")
                        .value(event.getAvailabilityZone()).build(),
                Dimension.builder()
                        .name("id")
                        .value(event.getId()).build(),
                Dimension.builder()
                        .name("instance_name")
                        .value(event.getInstanceName()).build(),
                Dimension.builder()
                        .name("process_name")
                        .value(event.getProcessName()).build(),
                Dimension.builder()
                        .name("jdk_version")
                        .value(event.getJdkVersion())
                        .build()
        );

        List<MeasureValue> measureValues = List.of(
                MeasureValue.builder()
                        .name("task_completed")
                        .type(MeasureValueType.BIGINT)
                        .value(Integer.toString(event.getTaskCompleted())).build(),
                MeasureValue.builder()
                        .name("task_end_state")
                        .type(MeasureValueType.VARCHAR)
                        .value(event.getTaskEndState()).build(),
                MeasureValue.builder()
                        .name("gc_reclaimed")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(event.getGcReclaimed())).build(),
                MeasureValue.builder()
                        .name("gc_pause")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(event.getGcPause())).build(),
                MeasureValue.builder()
                        .name("memory_free")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(event.getMemoryFree())).build()
        );

        return Record.builder()
                .dimensions(dimensions)
                .measureName("events_record")
                .measureValueType("MULTI")
                .measureValues(measureValues)
                .timeUnit(TimeUnit.SECONDS)
                .time(Long.toString(event.getTime())).build();
    }

    private static Record convertFromMetric(final MyHostMetric metric) {
        List<Dimension> dimensions = List.of(
                Dimension.builder()
                        .name("region")
                        .value(metric.getRegion()).build(),
                Dimension.builder()
                        .name("cell")
                        .value(metric.getCell()).build(),
                Dimension.builder()
                        .name("silo")
                        .value(metric.getSilo()).build(),
                Dimension.builder()
                        .name("availability_zone")
                        .value(metric.getAvailabilityZone()).build(),
                Dimension.builder()
                        .name("id")
                        .value(metric.getId()).build(),
                Dimension.builder()
                        .name("instance_type")
                        .value(metric.getInstanceType()).build(),
                Dimension.builder()
                        .name("os_version")
                        .value(metric.getOsVersion()).build(),
                Dimension.builder()
                        .name("instance_name")
                        .value(metric.getInstanceName()).build()
        );

        List<MeasureValue> measureValues = List.of(
                MeasureValue.builder()
                        .name("browser")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getBrowserType()).build(),
                MeasureValue.builder()
                        .name("browser_version")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getBrowserVersion()).build(),
                MeasureValue.builder()
                        .name("country_geo_ip")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getCountryGeoIp()).build(),
                MeasureValue.builder()
                        .name("device")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getDeviceType()).build(),
                MeasureValue.builder()
                        .name("evt_cat_1")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getEvtCat1()).build(),
                MeasureValue.builder()
                        .name("evt_cat_2")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getEvtCat2()).build(),
                MeasureValue.builder()
                        .name("evt_cat_3")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getEvtCat3()).build(),
                MeasureValue.builder()
                        .name("evt_type")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getEvtType()).build(),
                MeasureValue.builder()
                        .name("id")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getId()).build(),
                MeasureValue.builder()
                        .name("referral")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getReferral()).build(),
                MeasureValue.builder()
                        .name("session_id")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getSessionId()).build(),
                MeasureValue.builder()
                        .name("ts")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getTs()).build(),
                MeasureValue.builder()
                        .name("url")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getUrl()).build(),
                MeasureValue.builder()
                        .name("user_id")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getUserId()).build(),
                MeasureValue.builder()
                        .name("user_type")
                        .type(MeasureValueType.VARCHAR)
                        .value(metric.getUserType()).build(),
                MeasureValue.builder()
                        .name("disk_free")
                        .type(MeasureValueType.DOUBLE)
                        .value(doubleToString(metric.getDiskFree())).build()
        );


        return Record.builder()
                .dimensions(dimensions)
                .measureName("metrics_record")
                .measureValueType("MULTI")
                .measureValues(measureValues)
                .timeUnit(TimeUnit.SECONDS)
                .time(Long.toString(metric.getTime())).build();
    }

    private static String doubleToString(double inputDouble) {
        // Avoid sending -0.0 (negative double) to Timestream - it throws ValidationException
        if (Double.valueOf(-0.0).equals(inputDouble)) {
            return "0.0";
        }
        return Double.toString(inputDouble);
    }

}
