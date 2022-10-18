package io.substrait.spark;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.DebugUtils;
import org.apache.spark.sql.gluten.GlutenPlanner;
import org.apache.spark.sql.gluten.GlutenPlan;
import org.apache.spark.sql.gluten.ReplacePlaceHolder;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SparkSqlPlanTest extends BaseSparkSqlPlanTest {
  static final org.slf4j.Logger logger =
          org.slf4j.LoggerFactory.getLogger(SparkSqlPlanTest.class);
  public static void prepareSparkTables(SparkSession spark) throws IOException {
    File localWareHouseDir = new File("spark-warehouse");
    if (localWareHouseDir.exists()) {
      FileUtils.deleteDirectory(localWareHouseDir);
    }
    FileUtils.forceMkdir(localWareHouseDir);
    spark.sql("DROP DATABASE IF EXISTS tpch CASCADE");
    spark.sql("CREATE DATABASE IF NOT EXISTS tpch");
    spark.sql("use tpch");
    String tpchCreateTableString =
        FileUtils.readFileToString(
            new File("src/test/resources/tpch_schema.sql"), StandardCharsets.UTF_8);
    Arrays.stream(tpchCreateTableString.split(";"))
        .filter(StringUtils::isNotBlank)
        .toList()
        .forEach(spark::sql);
    spark.sql("show tables").show();
  }

  @BeforeAll
  public static void beforeAll() {
    spark =
            SparkSession.builder()
                .master("local[2]")
                .config("spark.sql.legacy.createHiveTableByDefault", "false")
                .config("spark.sql.codegen.wholeStage", "false")
                .config("spark.sql.adaptive.enabled", "false")
                .getOrCreate();
    try {
      prepareSparkTables(spark);
    } catch (IOException e) {
      Assertions.fail(e);
    }
  }

  @Test
  public void testReadRel() {
//    LogicalPlan plan = plan("select * from lineitem");
//    logger.info(plan.treeString());
//    logger.info(SparkLogicalPlanConverter.convert(plan).toString());
    SparkPlan pPlan= PhysicalPlan("select count(*)  + 1.0 from lineitem l inner join orders o on l.l_orderkey = o.o_orderkey");

    // SparkPlan pPlan= PhysicalPlan("select * from lineitem where l_orderkey > 0");
    logger.info("\n" + pPlan.treeString());
    GlutenPlanner planner = new GlutenPlanner();
    GlutenPlan newPlan = planner.plan(pPlan);
    logger.info("\n" + ((SparkPlan)newPlan).treeString());

    SparkPlan sparkPlan = ReplacePlaceHolder.apply(newPlan);
//
    logger.info("\n" + sparkPlan.treeString());

    logger.info("\n" + DebugUtils.printSubstraitPlan(sparkPlan));
  }
}
