package flink.exactlyonce;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckPointDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
// checkpoint的时间间隔，如果状态比较大，可以适当调大该值
        env.enableCheckpointing(1000);
// 配置处理语义，默认是exactly-once
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
// 两个checkpoint之间的最小时间间隔，防止因checkpoint时间过长，导致checkpoint积压
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
// checkpoint执行的上限时间，如果超过该阈值，则会中断checkpoint
        env.getCheckpointConfig().setCheckpointTimeout(60000);
// 最大并行执行的检查点数量，默认为1，可以指定多个，从而同时出发多个checkpoint，提升效率
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
// 设定周期性外部检查点，将状态数据持久化到外部系统中，
// 使用该方式不会在任务正常停止的过程中清理掉检查点数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
    }
}
