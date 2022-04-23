import org.scalatest.freespec.AnyFreeSpec
import pipeline.StreamingPipeline

class FirstPipelineTest extends AnyFreeSpec {

  "The first pipeline" - {
    "should do stuff" in {
      new StreamingPipeline().apply(SparkUtil.spark, PathUtil.getResourceParentFolderPath("streaming_pipeline"))

//      val actual = SparkUtil.spark.read.parquet("first_pipeline_output")
    }
  }
}
