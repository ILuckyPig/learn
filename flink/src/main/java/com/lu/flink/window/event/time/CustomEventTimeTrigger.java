package com.lu.flink.window.event.time;

import com.lu.util.DateUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimerServiceImpl;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamTaskNetworkInput;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator.Context;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;

import java.time.LocalDateTime;

/**
 * {@link StreamTaskNetworkInput#emitNext(DataOutput)} <br/>
 *  -> {@link StreamTaskNetworkInput#processElement(StreamElement, DataOutput)} <br/>
 *  -> {@link StatusWatermarkValve#inputWatermark(Watermark,int)} <br/>
 *  -> {@link StatusWatermarkValve#findAndOutputNewMinWatermarkAcrossAlignedChannels()} <br/>
 *  -> {@link OneInputStreamTask.StreamTaskNetworkOutput#emitWatermark(Watermark)} <br/>
 *  -> {@link AbstractStreamOperator#processWatermark(Watermark)} <br/>
 *  -> {@link InternalTimeServiceManager#advanceWatermark(Watermark)} <br/>
 *  -> {@link InternalTimerServiceImpl#advanceWatermark(long)} <br/>
 *  -> {@link Context#getCurrentWatermark()} <br/>
 *
 */
public class CustomEventTimeTrigger extends Trigger<Object, TimeWindow> {

    private CustomEventTimeTrigger() {}

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        Tuple3<String, LocalDateTime, Long> tuple3 = (Tuple3<String, LocalDateTime, Long>) element;
        System.out.printf("%s>> trigger   { timestamp=%s, date=%s | currentMaxTimestamp=%s, date=%s | currentWatermark=%s, date=%s | start=%s, end=%s }%n",
                Thread.currentThread().getId(),
                tuple3.f2,
                DateUtil.transFormat(tuple3.f2),
                window.maxTimestamp(),
                DateUtil.transFormat(window.maxTimestamp()),
                ctx.getCurrentWatermark(),
                DateUtil.transFormat(ctx.getCurrentWatermark()),
                DateUtil.transFormat(window.getStart()),
                DateUtil.transFormat(window.getEnd())
        );
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            return TriggerResult.FIRE;
        } else {
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        return time == window.maxTimestamp() ?
                TriggerResult.FIRE :
                TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window,
                        OnMergeContext ctx) {
        // only register a timer if the watermark is not yet past the end of the merged window
        // this is in line with the logic in onElement(). If the watermark is past the end of
        // the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "CustomEventTimeTrigger()";
    }

    /**
     * Creates an event-time trigger that fires once the watermark passes the end of the window.
     *
     * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
     * trigger window evaluation with just this one element.
     */
    public static CustomEventTimeTrigger create() {
        return new CustomEventTimeTrigger();
    }
}
