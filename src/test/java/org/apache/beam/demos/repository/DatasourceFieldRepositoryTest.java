package org.apache.beam.demos.repository;

import org.apache.beam.demos.MysqlDemoSpringBoot;
import org.apache.beam.demos.SpringTestBoot;
import org.apache.beam.demos.entity.DatasourceField;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Example;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

/**
 * @Description
 * @Author liuzhiqiang
 * @Date 2019/8/13 9:21
 */
@SpringBootTest(classes = SpringTestBoot.class)
@RunWith(SpringRunner.class)
public class DatasourceFieldRepositoryTest {
    @Autowired
    DatasourceFieldRepository datasourceFieldRepository;

    @Test
    public void testSelect(){
        DatasourceField ex = new DatasourceField();
        ex.setVersionId("3");
        ex.setIsStatus("1");
        List<DatasourceField> list = datasourceFieldRepository.findAll(Example.of(ex));
        Assert.assertEquals(168, list.size());
    }
}
