package org.apache.beam.demos.repository;

import org.apache.beam.demos.entity.DatasourceField;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/12 11:01
 */
@Repository
public interface DatasourceFieldRepository extends JpaRepository<DatasourceField, Integer> {

}
