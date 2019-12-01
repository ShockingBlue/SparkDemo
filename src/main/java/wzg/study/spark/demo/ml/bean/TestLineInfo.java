package wzg.study.spark.demo.ml.bean;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class TestLineInfo implements Serializable {
    private long id;
    private String text;

    public TestLineInfo() {
    }

    public TestLineInfo(long id, String text) {
        this.id = id;
        this.text = text;
    }
}
