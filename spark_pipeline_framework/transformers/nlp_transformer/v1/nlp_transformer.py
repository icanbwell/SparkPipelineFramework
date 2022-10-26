# noinspection PyProtectedMember

from pyspark.ml.functions import vector_to_array
from pyspark.sql.dataframe import DataFrame

from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from typing import Any, Dict, Optional
import pyspark.sql.functions as f
from pyspark.ml.feature import HashingTF, IDF, CountVectorizer

import sparknlp
from sparknlp.annotator import *
from sparknlp.base import *
from sparknlp.pretrained import PretrainedPipeline
from pyspark.ml import Pipeline


class NlpTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    """
    jar_packages = "mysql:mysql-connector-java:8.X.X,\
        com.johnsnowlabs.nlp:spark-nlp_2.X:4.X.X" #this is a string that behaves like a list

    Must add the following configuration to the initial spark code
        spark_session = SparkSession.builder \
        .appName(XXX) \
        .master("XXX") \
        .config("spark.driver.memory", "16G") \
        .config("spark.driver.maxResultSize", "0") \
        .config("spark.kryoserializer.buffer.max", "2000M") \
        .config("spark.jars.packages",
                jar_packages
                )
        .getOrCreate()

        One may have to add other jar packages, and add them to the jar_packages string (with a "," separating)

    For parameter "perform_analysis" the default is all of the analysis will be performed, but the user may select custom options.
    Options for parameter: `perform_analysis`
    - "all" : default parameter
    - "character_count"
    - "word_count"
    - "embeddings"
    - "sentiment"
    - "tf_idf"
    - "entities"
    - "bag_of_words"
    - "first_word"
    """

    @keyword_only
    def __init__(
            self,
            columns: str,
            view: str,
            binarize_tokens: Optional[bool] = True,
            perform_analysis: Optional[list] = ["all"],
            # add your parameters here
            name: Optional[str] = None,
            parameters: Optional[Dict[str, Any]] = None,
            progress_logger: Optional[ProgressLogger] = None,
    ):
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param

        assert columns
        assert view

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=None)

        self.columns: Param[str] = Param(self, "columns", "")
        self._setDefault(columns=columns)

        self.binarizeTokens: Param[bool] = Param(self, "binarizeTokens", "")
        self._setDefault(binarizeTokens=binarize_tokens)

        self.performAnalysis: Param(list[str]) = Param(self, "performAnalysis", "")
        self._setDefault(performAnalysis=perform_analysis)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:

        """
        Performs a series of NLP feature generations  for analysis in a spark Dataframe Environment.
        Loads end result to a Spark View

        :param DataFrame df: Dataframe for NLP Transformation
        """
        # get needed parameters
        columns = self.get_columns()
        view = self.get_view()
        perform_analysis = self.get_perform_analysis()
        if "all" in perform_analysis or len(perform_analysis) == 0:
            perform_all = True
        in_col = columns
        # get data from view
        # spark_session = df.sql_ctx.sparkSession

        # spark_session.builder.config("spark.jars.packages", "com.johnsnowlabs.nlp_transformer:spark-nlp_2.12:4.2.1")

        df_nlp: DataFrame = df.sql_ctx.table(view)
        final_columns = []
        explode_vector_tf = True  # need to functionize

        if "id" in df_nlp.columns:
            df_nlp = df_nlp.select("id", columns)
            final_columns.append("id")
        else:
            df_nlp = df_nlp.select(columns)


        final_columns.append(in_col)

        ################
        # TOKENIZER PIPELINE
        df_nlp = df_nlp.dropna(subset=[in_col])
        #create text column with original text, which is necesarry for some pretrained pipelines
        df_nlp = df_nlp.withColumn("text", df_nlp[columns])
        out_col = "ntokens"
        nlp_pipeline = self.prepare_for_nlp(in_col, out_col)
        nlp_model = nlp_pipeline.fit(df_nlp)
        df_nlp = nlp_model.transform(df_nlp)
        final_columns.append(out_col)
        ################

        ################
        # SPARK-NLP SENTIMENTIZER
        out_col = "sentiment"
        if out_col in perform_analysis or perform_all:
            sentiment_pipeline = self.sentiment_vivenk(document_col="document", normalization_col="normalized",
                                                       output_col=out_col)
            sentiment_model = sentiment_pipeline.fit(df_nlp)
            df_nlp = sentiment_model.transform(df_nlp).persist()
            final_columns.append(out_col)

        ############
        # FEATURE GEN
        # get string length
        out_col = "character_count"
        if out_col in perform_analysis or perform_all:
            df_nlp = df_nlp.withColumn(out_col, f.length(in_col))
            final_columns.append(out_col)

        # get word count
        out_col = "word_count"
        if out_col in perform_analysis or perform_all:
            df_nlp = df_nlp.withColumn(out_col, f.size(f.split(f.col(in_col), " ")))
            final_columns.append(out_col)

        # get embeddings
        out_col = "embeddings"
        if out_col in perform_analysis or perform_all:
            embeddings_pipeline = self.sentence_embeddings()
            embeddings_model = embeddings_pipeline.fit(df_nlp)
            df_nlp = embeddings_model.transform(df_nlp)
            final_columns.append(out_col)

        # get tfidf
        out_col = "tf_idf"
        if out_col in perform_analysis or perform_all:
            tfidf = self.get_tfidf(output_col=out_col)
            tfidf_model = tfidf.fit(df_nlp)
            df_nlp = tfidf_model.transform(df_nlp)
            final_columns.append(out_col)

        # do entity recognition
        out_col = "entities"
        if out_col in perform_analysis or "all" in perform_analysis:

            ner = self.get_entities()
            #ner_model = ner.fit(df_nlp)
            df_nlp = ner.transform(df_nlp)
            final_columns.append(out_col)

        # label binarizer
        # get bag of words
        out_col = "bag_of_words"
        if out_col in perform_analysis or perform_all:
            explode_col = "binarize_bag_of_words"
            df_nlp = self.do_bag_of_words(df=df_nlp, tokens_col="ntokens", out_col=out_col,
                                          explode_vector=explode_vector_tf,
                                          explode_col=explode_col)

            final_columns.append(out_col)
            final_columns.append(explode_col)

        # first word binarizer

        # first word
        out_col = "first_word"
        if out_col in perform_analysis or perform_all:
            out_col_fw = "first_word0"
            out_col_fwl = out_col_fw + "_L"
            in_col_t = "ntokens"

            # get first word from token list
            df_nlp = df_nlp.withColumn(out_col_fw, df_nlp[in_col_t].getItem(0))
            # convert to a list so it will work with our bag of words code
            df_nlp = df_nlp.withColumn(out_col_fwl, f.split(out_col_fw, ","))
            explode_col = "binarize_first_word"
            df_nlp = self.do_bag_of_words(df=df_nlp, tokens_col=out_col_fwl, out_col=out_col,
                                          explode_vector=explode_vector_tf, explode_col=explode_col)
            final_columns.append(out_col)
            final_columns.append(explode_col)

        ##################
        # finalizing output
        df_nlp = df_nlp.select(final_columns)

        df_nlp.createOrReplaceTempView(view)  # loading to view
        # df_result.createOrReplaceTempView(view)
        return df_nlp

    def get_columns(self) -> str:
        return self.getOrDefault(self.columns)

    def get_view(self) -> str:
        return self.getOrDefault(self.view)

    def get_binarize_tokens(self) -> Optional[bool]:
        return self.getOrDefault(self.binarizeTokens)

    def get_perform_analysis(self) -> list:
        return self.getOrDefault(self.performAnalysis)

    def prepare_for_nlp(self, input_col: str = "text", output_col: str = "ntokens") -> Pipeline:
        """
        Preforms a multi-step process to generate many of the column entities needed for NLP Analysis.
        DocumentAssembler--> SentenceDetector--> Tokenizer --> Stemmer--> Normalizer--> Finisher

        :param str input_col: Text column that is the target of the analysis
        :param str output_col: Name of finished token output column

        :rtype: Pipeline
        """

        pipeline_list = []
        document_assembler = DocumentAssembler() \
            .setInputCol(input_col) \
            .setOutputCol("document")
        pipeline_list.append(document_assembler)

        sentence_detector = SentenceDetector() \
            .setInputCols(["document"]) \
            .setOutputCol("sentence") \
            .setUseAbbreviations(True)
        pipeline_list.append(sentence_detector)

        tokenizer = Tokenizer() \
            .setInputCols(["sentence"]) \
            .setOutputCol("token")
        pipeline_list.append(tokenizer)

        stemmer = Stemmer() \
            .setInputCols(["token"]) \
            .setOutputCol("stem")
        pipeline_list.append(stemmer)

        normalizer = Normalizer() \
            .setInputCols(["stem"]) \
            .setOutputCol("normalized")
        pipeline_list.append(normalizer)

        finisher = Finisher() \
            .setInputCols(["normalized"]) \
            .setOutputCols([output_col]) \
            .setOutputAsArray(True) \
            .setCleanAnnotations(False)
        pipeline_list.append(finisher)

        nlp_pipeline = Pipeline(stages=pipeline_list)
        return nlp_pipeline

    def sentiment_vivenk(self, document_col="document", normalization_col="normalized", output_col="final_sentiment") -> Pipeline:
        """
        Generates a pipeline that creates sentiment values from documents and normalization columns

        :param str document_col: Column with documents typically created with documentAssembler()
        :param str normalization_col: Name of input column with tokens typically created with Normalizer()
        :param str output_col: Name of sentiment output column

        :rtype: Pipeline
        """
        vivekn = ViveknSentimentModel.pretrained() \
            .setInputCols([document_col, normalization_col]) \
            .setOutputCol("result_sentiment")

        finisher = Finisher() \
            .setInputCols(["result_sentiment"]) \
            .setOutputCols(output_col) \
            .setCleanAnnotations(False)

        pipeline = Pipeline().setStages([vivekn, finisher])
        return pipeline

    def sentiment_pretrained(self, sentiment_model: str = 'analyze_sentiment') -> PretrainedPipeline:
        """
        Performs Sentiment Analysis
        :param str sentiment_model: The pretrained model that is used for analysis.
        :rtype: PretrainedPipeline

        Possible configurations for sentiment model:
         -- "analyze_sentiment"
         -- "analyze_sentimentdl_glove_imdb"
         -- "analyze_sentimentdl_use_twitter"
        """
        pipeline = PretrainedPipeline(sentiment_model, lang='en')
        return pipeline

    def sentence_embeddings(self, document_col="document", token_col="token", output_col="embeddings") -> Pipeline:
        """
        Generates a pipeline that creates sentence embeddings transformer

        :param str document_col: Column with documents typically created with documentAssembler()
        :param str token_col: Name of input column with tokens typically created with Tokenizer()
        :param str output_col: Name of embeddings output column

        :rtype: Pipeline
        """
        pipeline_list = []
        embeddings = WordEmbeddingsModel.pretrained() \
            .setInputCols(document_col, token_col) \
            .setOutputCol("embeddings0")
        pipeline_list.append(embeddings)

        embeddings_sentence = SentenceEmbeddings() \
            .setInputCols([document_col, "embeddings0"]) \
            .setOutputCol("sentence_embeddings") \
            .setPoolingStrategy("AVERAGE")
        pipeline_list.append(embeddings_sentence)

        # puts embeddings into an anlaysis ready form
        embeddings_finisher = EmbeddingsFinisher() \
            .setInputCols("sentence_embeddings") \
            .setOutputCols(output_col) \
            .setOutputAsVector(True) \
            .setCleanAnnotations(False)
        pipeline_list.append(embeddings_finisher)

        nlp_pipeline = Pipeline(stages=pipeline_list)  # [embeddings, embeddingsSentence, embeddingsFinisher]
        return nlp_pipeline

    def do_bag_of_words(self, df, tokens_col: str = "ntokens", out_col: str = "bag_of_words_vectors",
                        explode_col: str = "binarized_bag_of_words",
                        explode_vector: bool = True
                       ) -> DataFrame:
        """
        Generates a transformer that creates a bag of words analysis from a list of tokens

        :param DataFrame df: The person sending the message
        :param str tokens_col: Column with array of tokens
        :param str out_col: Name of vector output column
        :param str explode_col: Name of exploded vector column
        :param bool explode_vector: Whether to create the exploded column
        :rtype: DataFrame
        """

        cv = CountVectorizer()
        cv.setInputCol(tokens_col)
        cv.setOutputCol(out_col)
        model = cv.fit(df)
        df = model.transform(df)
        if explode_vector:
            df = df.withColumn(explode_col, vector_to_array(out_col))

        return df

    def get_tfidf(
            self,
            token_col: str = "ntokens",
            output_col: str = "tf_idf_features",
    ) -> Pipeline:
        """
        Generates a tf-idf analysis.

        :param str tokens_col: Column with array of tokens
        :param str out_col: Name of vector output column
        :rtype: Pipeline
        """
        hashing_output_name = "tf_idf_raw_features"
        tf_hash = HashingTF() \
            .setInputCol(token_col) \
            .setOutputCol(hashing_output_name)  # inputCol=token_col, outputCol=hashing_output_name)\

        idf = IDF() \
            .setInputCol(hashing_output_name) \
            .setOutputCol(output_col)  # . =hashing_output_name, outputCol=output_col)
        tfidf_pipeline = Pipeline(stages=[tf_hash, idf])

        return tfidf_pipeline


    def get_entities(self,  entity_pipeline:str = "onto_recognize_entities_sm") -> PretrainedPipeline:

        """
        Creates transformer for pretrained entity recognition pipeline. In a Spark DataFrame environment.
        Code looks for "text" column
        Options for `entity_pipeline` are:
        - "onto_recognize_entities_sm"
        - "onto_recognize_entities_bert_tiny"

        :param str entity_pipeline: The name of the pretrained pipeline to use
        :rtype PretrainedPipeline
        """
        #pipeline = PretrainedPipeline("onto_recognize_entities_sm")

        pipeline = PretrainedPipeline(entity_pipeline)

        return pipeline

