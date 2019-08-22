package org.apache.beam.demos.entity;

import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.io.Serializable;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/12 10:55
 */
@Entity
@Table(name = "dim_datasource_field_define")
@Getter
@Setter
public class DatasourceField implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name="field_id")
    private Integer fieldId;

    @Column(name = "field_english_name")
    private String fieldEnglishName;

    @Column(name = "data_address")
    private String dataAddress;

    @Column(name = "version_id")
    private String versionId;

    @Column(name = "is_status")
    private String isStatus;
}
