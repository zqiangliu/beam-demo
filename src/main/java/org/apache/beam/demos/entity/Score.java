package org.apache.beam.demos.entity;

import lombok.Data;
import org.apache.beam.demos.common.ScoreCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/19 9:16
 */
@Data
//@DefaultCoder(ScoreCoder.class)
public class Score implements Serializable {
    private Long studentId;
    private Long subjectId;
    private Float score;

    public Score(Long studentId, Long subjectId, Float score) {
        this.studentId = studentId;
        this.subjectId = subjectId;
        this.score = score;
    }
}
