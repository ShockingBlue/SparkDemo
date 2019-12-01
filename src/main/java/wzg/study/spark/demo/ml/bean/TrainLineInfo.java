package wzg.study.spark.demo.ml.bean;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class TrainLineInfo implements Serializable {
    private long id;
    private String text;
    private long label;

    public TrainLineInfo() {
        id = 0L;
        text = "";
        label = 0L;
    }

    public TrainLineInfo(long id, String text, long label) {
        this.id = id;
        this.text = text;
        this.label = label;
    }
}
