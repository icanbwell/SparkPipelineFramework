# noinspection PyProtectedMember
from pyspark.ml.functions import vector_to_array
from pyspark.sql.dataframe import DataFrame
from sparknlp import EmbeddingsFinisher, DocumentAssembler, Finisher  # type: ignore
from sparknlp.annotator import (  # type: ignore
    SentenceEmbeddings,
    WordEmbeddingsModel,
    SentenceDetector,
    Stemmer,
    ViveknSentimentModel,
    Tokenizer,
    Normalizer,
    SentimentDetector,
    UniversalSentenceEncoder,
    ClassifierDLModel,
    Lemmatizer,
    SentimentDLModel,
)
import pyspark.sql.functions as f
from pyspark.ml.feature import HashingTF, IDF, CountVectorizer
from sparknlp.pretrained import PretrainedPipeline  # type: ignore
from pyspark.ml import Pipeline

from pyspark.ml.param import Param
from spark_pipeline_framework.logger.yarn_logger import get_logger
from spark_pipeline_framework.progress_logger.progress_logger import ProgressLogger
from spark_pipeline_framework.transformers.framework_transformer.v1.framework_transformer import (
    FrameworkTransformer,
)
from typing import Any, Dict, Optional, List
from spark_pipeline_framework.utilities.capture_parameters import capture_parameters


class NlpTransformer(FrameworkTransformer):
    # noinspection PyUnusedLocal
    @capture_parameters  # @keyword_only
    def __init__(
        self,
        column: str,
        view: str,
        binarize_tokens: bool = True,
        tfidf_n_features: Optional[int] = None,
        condense_output_columns: bool = True,
        perform_analysis: List[str] = ["all"],
        # add your parameters here
        name: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        progress_logger: Optional[ProgressLogger] = None,
    ):
        """

        :param column: string column of interest to perform NLP analysis
        :param view: the view to load the data into
        :param binarize_tokens: bool whether to include the arrays for binarizing labels in addition to the vectors.
            Default: True
        :param tfidf_n_features: set N most used features in text for tf-idf analysis. Default: None (use all features)
        :param condense_output_columns: remove all non-final columns used in data processing. Default True
        :param perform_analysis: the analysis options that the user wants to be performed. Here are the options
            - "all" : default parameter
            - "character_count"
            - "word_count"
            - "sentence_embeddings"
            - "sentiment"
            - "tf_idf"
            - "bag_of_words"
            - "first_word"
        :param name: a name to use when logging information about this transformer
        :param parameters: the parameter dictionary
        :param progress_logger: the progress logger


        # SETUP HELP
        Must add the following configuration to the initial spark code
        #this jar-string behaves like a list on the backend
        One may have to add other jar packages, and add them to the jar_packages string (with a "," separating)

        # jar_packages = "mysql:mysql-connector-java:X.X.X,\
        #    com.johnsnowlabs.nlp:spark-nlp_X.X:X.X.X,\
             com.someOtherJarY:X.X.X"

        # the following works as of November 2022


        jar_packages = "mysql:mysql-connector-java:8.0.24,\
            com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.1"


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

        Above are the recommended options by John Snow Labs.
        The following works without some of the options as the least intrusive possible code

        spark_session = SparkSession.builder \
            .appName(XXX) \
            .master("XXX") \
            .config("spark.jars.packages",
                    jar_packages
                    )
            .getOrCreate()



        # About `perform_analysis`

        For parameter "perform_analysis" the default is all the analysis will be performed, but can select custom option
        Options for parameter: `perform_analysis`
        - "all" : default parameter
        - "character_count"
        - "word_count"
        - "sentence_embeddings"
        - "sentiment"
        - "tf_idf"
        - "bag_of_words"
        - "bag_of_words_stem"
        - "first_word"

        """
        super().__init__(
            name=name, parameters=parameters, progress_logger=progress_logger
        )

        self.logger = get_logger(__name__)

        # add a param

        assert column
        assert view

        self.view: Param[str] = Param(self, "view", "")
        self._setDefault(view=None)

        self.column: Param[str] = Param(self, "column", "")
        self._setDefault(column=column)

        self.binarize_tokens: Param[bool] = Param(self, "binarize_tokens", "")
        self._setDefault(binarize_tokens=binarize_tokens)

        self.perform_analysis: Param[List[str]] = Param(self, "perform_analysis", "")
        self._setDefault(perform_analysis=perform_analysis)

        self.tfidf_n_features: Param[int] = Param(self, "tfidf_n_features", "")
        self._setDefault(tfidf_n_features=tfidf_n_features)

        self.condense_output_columns: Param[bool] = Param(
            self, "condense_output_columns", ""
        )
        self._setDefault(condense_output_columns=condense_output_columns)

        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    def _transform(self, df: DataFrame) -> DataFrame:

        """
        Performs a series of NLP feature generations  for analysis in a spark Dataframe Environment.
        Loads end result to a Spark View

        :param DataFrame df: Dataframe for NLP Transformation
        """
        # get needed parameters
        column = self.get_column()
        view = self.get_view()
        perform_analysis = self.get_perform_analysis()
        tfidf_n_features = self.get_tfidf_n_features()
        condense_output_columns = self.get_condense_output_columns()
        # if the designation is "all" or the list is empty or non-existent.
        # perform all is boolean that will compile a dataframe with every analysis for the intended column
        if "all" in perform_analysis or len(perform_analysis) == 0:
            perform_all = True
        else:
            perform_all = False
        in_col = column
        # get data from view
        #
        df_nlp: DataFrame = df.sql_ctx.table(view)
        final_columns = []
        explode_vector_tf = True  # need to make into function

        if "id" in df_nlp.columns:
            df_nlp = df_nlp.select("id", column)
            final_columns.append("id")
        else:
            df_nlp = df_nlp.select(column)

        final_columns.append(in_col)
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
        ################
        # TOKENIZER PIPELINE
        df_nlp = df_nlp.dropna(subset=[in_col])
        # create text column with original text, which is necessary for some pretrained pipelines
        df_nlp = df_nlp.withColumn("text", df_nlp[column])
        out_col = "ntokens"
        nlp_pipeline = self.do_prepare_for_nlp(in_col, out_col)
        nlp_model = nlp_pipeline.fit(df_nlp)
        df_nlp = nlp_model.transform(df_nlp)
        final_columns.append(out_col)
        ################

        ################
        # SPARK-NLP SENTIMENTIZER
        out_col = "sentiment"
        if out_col in perform_analysis or perform_all:
            sentiment_pipeline = self.get_sentiment_vivenk(
                document_col="document",
                normalization_col="normalized",
                output_col=out_col,
            )
            sentiment_model = sentiment_pipeline.fit(df_nlp)
            df_nlp = sentiment_model.transform(df_nlp).persist()
            final_columns.append(out_col)
        """
        ################
        # SPARK-NLP Emotion Classifier
        out_col = "emotion_classifier"

        if out_col in perform_analysis or perform_all:
            begin = time.time()
            # print("Performing Analysis: {}".format(out_col))
            # emotion_pipeline = self.get_emotions_tfhub(
            #    document_col="document", output_col=out_col)
            # emotion_model = emotion_pipeline.fit(df_nlp)
            # df_nlp = emotion_model.transform(df_nlp).persist()
            # final_columns.append(out_col)
            # print("--Time Elapsed: ", round(time.time() - begin, 2))
        ############

        ################
        # SPARK-NLP SENTIMENTIZER
        out_col = "sentiment_score"
        if out_col in perform_analysis or perform_all:
            pretrained_sentiment_pipeline = self.sentiment_pretrained()
            sentiment_model = pretrained_sentiment_pipeline.fit(df_nlp)
            df_nlp = sentiment_model.transform(df_nlp)
            prediction = sentiment_model.transform(df_nlp)
            prediction.select(out_col + ".metadata").show(truncate=False)

            final_columns.append(out_col)
        """

        # get embeddings
        out_col = "sentence_embeddings"
        if out_col in perform_analysis or perform_all:
            embeddings_pipeline = self.get_sentence_embeddings_pipeline()
            embeddings_model = embeddings_pipeline.fit(df_nlp)
            df_nlp = embeddings_model.transform(df_nlp)
            final_columns.append(out_col)

        # get tfidf
        out_col = "tf_idf"
        if out_col in perform_analysis or perform_all:
            tfidf = self.get_tfidf(
                output_col=out_col, tfidf_n_features=tfidf_n_features
            )
            tfidf_model = tfidf.fit(df_nlp)
            df_nlp = tfidf_model.transform(df_nlp)
            final_columns.append(out_col)
        """
        #11/9 Commented out to assist pytest 
        # do entity recognition
        out_col = "entities"
        if out_col in perform_analysis or "all" in perform_analysis:
            ner = self.get_entities()
            # ner_model = ner.fit(df_nlp)
            df_nlp = ner.transform(df_nlp)
            final_columns.append(out_col)
        """
        # label binarizer
        # get bag of words
        out_col = "bag_of_words"
        if out_col in perform_analysis or perform_all:
            explode_col = "binarize_bag_of_words"
            df_nlp = self.do_bag_of_words(
                df=df_nlp,
                tokens_col="ntokens",
                out_col=out_col,
                explode_vector=explode_vector_tf,
                explode_col=explode_col,
            )

            final_columns.append(out_col)
            final_columns.append(explode_col)

        out_col = "bag_of_words_stem"
        if out_col in perform_analysis or perform_all:
            explode_col = "binarize_bag_of_words_stem"
            df_nlp = self.do_bag_of_words(
                df=df_nlp,
                tokens_col="ntokens_stem",
                out_col=out_col,
                explode_vector=explode_vector_tf,
                explode_col=explode_col,
            )

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
            # convert to a list, then it will work with our bag of words codes
            df_nlp = df_nlp.withColumn(out_col_fwl, f.split(out_col_fw, ","))
            explode_col = "binarize_first_word"
            df_nlp = self.do_bag_of_words(
                df=df_nlp,
                tokens_col=out_col_fwl,
                out_col=out_col,
                explode_vector=explode_vector_tf,
                explode_col=explode_col,
            )
            final_columns.append(out_col)
            final_columns.append(explode_col)

        ##################
        # finalizing output
        if condense_output_columns:
            print("##########")
            print("Condensing Output")
            df_nlp = df_nlp.select(final_columns)
            print("##########")

        df_nlp.createOrReplaceTempView(view)  # loading to view
        # df_result.createOrReplaceTempView(view)
        return df_nlp

    def do_prepare_for_nlp(
        self, input_col: str = "text", output_col: str = "ntokens"
    ) -> Pipeline:
        """
        Preforms a multistep process to generate many of the column entities needed for NLP Analysis.
        DocumentAssembler--> SentenceDetector--> Tokenizer --> Stemmer--> Normalizer--> Finisher

        :param str input_col: Text column that is the target of the analysis
        :param str output_col: Name of finished token output column

        :rtype: Pipeline
        """

        pipeline_list = []
        document_assembler = (
            DocumentAssembler().setInputCol(input_col).setOutputCol("document")
        )
        pipeline_list.append(document_assembler)

        sentence_detector = (
            SentenceDetector()
            .setInputCols(["document"])
            .setOutputCol("sentence")
            .setUseAbbreviations(True)
        )
        pipeline_list.append(sentence_detector)

        tokenizer = Tokenizer().setInputCols(["sentence"]).setOutputCol("token")
        pipeline_list.append(tokenizer)

        stemmer = Stemmer().setInputCols(["token"]).setOutputCol("stem")
        pipeline_list.append(stemmer)

        normalizer = Normalizer().setInputCols(["stem"]).setOutputCol("normalized")
        pipeline_list.append(normalizer)

        normalizer_token = (
            Normalizer()
            .setInputCols(["token"])
            .setOutputCol("normalized_token")
            .setLowercase(True)
        )
        pipeline_list.append(normalizer_token)

        output_col_stem = output_col + "_stem"
        finisher = (
            Finisher()
            .setInputCols(["normalized"])
            .setOutputCols([output_col_stem])
            .setOutputAsArray(True)
            .setCleanAnnotations(False)
        )
        pipeline_list.append(finisher)

        finished_token = (
            Finisher()
            .setInputCols(["normalized_token"])
            .setOutputCols([output_col])
            .setOutputAsArray(True)
            .setCleanAnnotations(False)
        )
        pipeline_list.append(finished_token)

        nlp_pipeline = Pipeline(stages=pipeline_list)
        return nlp_pipeline

    def get_sentiment_vivenk(
        self,
        document_col: str = "document",
        normalization_col: str = "normalized",
        output_col: str = "final_sentiment",
    ) -> Pipeline:
        """
        Generates a pipeline that creates sentiment values from documents and normalization columns

        :param str document_col: Column with documents typically created with documentAssembler()
        :param str normalization_col: Name of input column with tokens typically created with Normalizer()
        :param str output_col: Name of sentiment output column

        :rtype: Pipeline
        """
        vivekn = (
            ViveknSentimentModel.pretrained()
            .setInputCols([document_col, normalization_col])
            .setOutputCol("result_sentiment")
        )
        finisher = (
            Finisher()
            .setInputCols(["result_sentiment"])
            .setOutputCols(output_col)
            .setCleanAnnotations(False)
        )

        pipeline = Pipeline().setStages([vivekn, finisher])
        return pipeline

    """
    def sentiment_pretrained(
            self,
            document_col: str = "documents",
            output_col: str = "sentiment_score",
            sentiment_model: str = "analyze_sentimentdl_glove_imdb",
    ) -> PretrainedPipeline:
        #""
        Performs Sentiment Analysis
        :param str document_col: Column with documents typically created with documentAssembler()
        :param str output_col: Name of sentiment output column
        :param str sentiment_model: The pretrained model that is used for analysis.
        :rtype: PretrainedPipeline

        Possible configurations for sentiment model:
         -- "analyze_sentiment"
         -- "analyze_sentimentdl_glove_imdb"
         -- "analyze_sentimentdl_use_twitter"
         -- "sentimentdl_use_imdb"
         -- "sentimentdl_use_twitter"
        #""
        use = (
            UniversalSentenceEncoder.pretrained()
            .setInputCols([document_col])
            .setOutputCol("sentence_embeddings")
        )

        sentimentdl = (
            SentimentDLModel.load("analyze_sentimentdl_use_twitter")
            .setInputCols(["sentence_embeddings"])
            .setOutputCol(output_col)
        )

        nlpPipeline = Pipeline(stages=[use, sentimentdl])
        # example
        #        # pipeline = PretrainedPipeline("onto_recognize_entities_sm")

        # pipeline = PretrainedPipeline(sentiment_model)
        return nlpPipeline
    """

    def get_sentence_embeddings_pipeline(
        self,
        document_col: str = "document",
        token_col: str = "token",
        output_col: str = "sentence_embeddings",
    ) -> Pipeline:
        """
        Generates a pipeline that creates sentence embeddings transformer

        :param str document_col: Column with documents typically created with documentAssembler()
        :param str token_col: Name of input column with tokens typically created with Tokenizer()
        :param str output_col: Name of embeddings output column

        :rtype: Pipeline
        """
        pipeline_list = []
        embeddings = (
            WordEmbeddingsModel.pretrained()
            .setInputCols(document_col, token_col)
            .setOutputCol("embeddings0")
        )
        pipeline_list.append(embeddings)

        embeddings_sentence = (
            SentenceEmbeddings()
            .setInputCols([document_col, "embeddings0"])
            .setOutputCol("sentence_embeddings0")
            .setPoolingStrategy("AVERAGE")
        )
        pipeline_list.append(embeddings_sentence)

        # puts embeddings into an analysis ready form
        embeddings_finisher = (
            EmbeddingsFinisher()
            .setInputCols("sentence_embeddings0")
            .setOutputCols(output_col)
            .setOutputAsVector(True)
            .setCleanAnnotations(False)
        )
        pipeline_list.append(embeddings_finisher)

        nlp_pipeline = Pipeline(
            stages=pipeline_list
        )  # [embeddings, embeddingsSentence, embeddingsFinisher]
        return nlp_pipeline

    def do_bag_of_words(
        self,
        df: DataFrame,
        tokens_col: str = "ntokens",
        out_col: str = "bag_of_words_vectors",
        explode_col: str = "binarized_bag_of_words",
        explode_vector: bool = True,
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
            df = df.withColumn(
                explode_col, vector_to_array(df[out_col])
            )  # arg-type: ignore

        return df

    def get_tfidf(
        self,
        token_col: str = "ntokens",
        output_col: str = "tf_idf_features",
        tfidf_n_features: Optional[int] = None,
    ) -> Pipeline:
        """
        Generates a tf-idf analysis.

        :param str token_col: Column with array of tokens
        :param str output_col: Name of vector output column
        :param str tfidf_n_features: set N most used features in text for tf-idf analysis. Default: use all features
        :rtype: Pipeline
        """
        hashing_output_name = "tf_idf_raw_features"
        tf_hash = HashingTF().setInputCol(token_col).setOutputCol(hashing_output_name)
        if tfidf_n_features:
            tf_hash.setNumFeatures(tfidf_n_features)
        idf = IDF().setInputCol(hashing_output_name).setOutputCol(output_col)
        tfidf_pipeline = Pipeline(stages=[tf_hash, idf])

        return tfidf_pipeline

    """
    def get_entities(
        self, entity_pipeline: str = "onto_recognize_entities_sm"
    ) -> PretrainedPipeline:

        ""#"
        Creates transformer for pretrained entity recognition pipeline. In a Spark DataFrame environment.
        Code looks for "text" column
        Options for `entity_pipeline` are:
        - "onto_recognize_entities_sm"
        - "onto_recognize_entities_bert_tiny"

        :param str entity_pipeline: The name of the pretrained pipeline to use
        :rtype PretrainedPipeline
        ""#"
        # pipeline = PretrainedPipeline("onto_recognize_entities_sm")

        pipeline = PretrainedPipeline(entity_pipeline)

        return pipeline
    """

    def get_emotions_tfhub(
        self, document_col: str = "document", output_col: str = "emotion_classifier"
    ) -> Pipeline:
        """
        Can classify various emotions in text.
        - Surprise
        - Sadness
        - Fear
        - Joy

        :param str document_col: contains the DocumentAssembler column name
        :param str output_col: Name of embeddings output column
        :rtype Pipeline
        """
        universal_sentence_encoder = (
            UniversalSentenceEncoder.pretrained("tfhub_use", lang="en")
            .setInputCols([document_col])
            .setOutputCol("sentence_embeddings")
        )

        emotion_classifier = (
            ClassifierDLModel.pretrained("classifierdl_use_emotion", "en")
            .setInputCols([document_col, "sentence_embeddings"])
            .setOutputCol(output_col)
        )

        emotion_classifier_pipeline = Pipeline(
            stages=[universal_sentence_encoder, emotion_classifier]
        )

        return emotion_classifier_pipeline

    # parameter getters
    def get_column(self) -> str:
        return self.getOrDefault(self.column)

    def get_view(self) -> str:
        return self.getOrDefault(self.view)

    def get_binarize_tokens(self) -> bool:
        return self.getOrDefault(self.binarize_tokens)

    def get_perform_analysis(self) -> List[str]:
        return self.getOrDefault(self.perform_analysis)

    def get_tfidf_n_features(self) -> int:
        return self.getOrDefault(self.tfidf_n_features)

    def get_condense_output_columns(self) -> bool:
        return self.getOrDefault(self.condense_output_columns)
