package videogames

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

object cleanAndQueryVideogames {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("LibroSpark")
      .master("local[2]")
      .getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    val df = spark.read.text("src/main/resources/games-data.csv")

    //Visualizar el DataFrame tal antes de estructurarlo.
    df.show(10, truncate = false)

    //Inferir esquema y añadir opciones
    val gamesdf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("quote", "\"")
      .load("src/main/resources/games-data.csv")

    //Comprobar si se ha cargado correctamente y mostrar el esquema
    gamesdf.show(10, truncate = false)
    gamesdf.printSchema()

    //Limpieza inicial del DataFrame haciendo lo siguiente:
    //Pasar la columna r-date a tipo Date
    //Multiplicar la columna user score por 10 (pasándola a Double) y luego castearla a Integer
    //Eliminar duplicados y convertir en array de strings la columna genre para poder trabajar con ella.
    val noduplicatesUdf = udf((x: Seq[String]) => x.distinct)
    val initialgamesDf = gamesdf
      .withColumn("r-date", to_date(gamesdf("r-date"), "MMMM dd, yyyy"))
      .withColumn("user score", (col("user score").cast("double")*10).cast("integer"))
      .withColumn("genre", regexp_replace(col("genre"), "[ ,]+", ","))
      .withColumn("genre", split(col("genre"), ","))
      .withColumn("genre", noduplicatesUdf(col("genre")))
    initialgamesDf.show(50, truncate = false)

    initialgamesDf.printSchema()
    //Quitar espacios en blanco
    val gamesDf1 = initialgamesDf.select(initialgamesDf.schema.fields.map { field =>
      if (field.dataType == StringType) {
        trim(col(field.name)).alias(field.name)
      } else {
        col(field.name)
      }
    }: _*)
    gamesDf1.show(11, truncate = false)

    //Limpiar la columna players por pasos
    //Visualizar cuántos valores distintos hay y cuántas ocurrencias tiene cada valor:
    gamesDf1.groupBy("players")
      .agg(count("players").as("ocurrencias"))
      .orderBy("players")
      .show(100, truncate = false)

    //Agrupar para minimizar el número de valores distintos.
    //Lo hago con el método .equalTo en lugar de con == y en una columna nueva para poder hacer después otras transformaciones sobre players

    val gamesDf2 = gamesDf1.withColumn("maxplayers",
      when(col("players").equalTo("1 Player"), "1")
        .when(
          col("players").equalTo("1-2")
            .or(col("players").equalTo("2"))
            .or(col("players").equalTo("2 Online")), "2")
        .when(
          col("players").equalTo("1-3")
            .or(col("players").equalTo("3 Online"))
            .or(col("players").equalTo("Up to 3")), "3")
        .when(
          col("players").equalTo("1-4")
            .or(col("players").equalTo("4 Online"))
            .or(col("players").equalTo("Up to 4")), "4")
        .when(
          col("players").equalTo("1-5")
            .or(col("players").equalTo("5 Online"))
            .or(col("players").equalTo("Up to 5"))
            .or(col("players").equalTo("1-10"))
            .or(col("players").equalTo("1-6"))
            .or(col("players").equalTo("1-10"))
            .or(col("players").equalTo("1-8"))
            .or(col("players").equalTo("10  Online"))
            .or(col("players").equalTo("6  Online"))
            .or(col("players").equalTo("8  Online"))
            .or(col("players").equalTo("Up to 6"))
            .or(col("players").equalTo("Up to 8"))
            .or(col("players").equalTo("Up to 9")), "5-10")
        .when(
          col("players").equalTo("1-12")
            .or(col("players").equalTo("1-16"))
            .or(col("players").equalTo("1-24"))
            .or(col("players").equalTo("1-32"))
            .or(col("players").equalTo("1-64"))
            .or(col("players").equalTo("12  Online"))
            .or(col("players").equalTo("14  Online"))
            .or(col("players").equalTo("16  Online"))
            .or(col("players").equalTo("24  Online"))
            .or(col("players").equalTo("32  Online"))
            .or(col("players").equalTo("44  Online"))
            .or(col("players").equalTo("64  Online"))
            .or(col("players").equalTo("Up to 12"))
            .or(col("players").equalTo("Up to 14"))
            .or(col("players").equalTo("Up to 16"))
            .or(col("players").equalTo("Up to 18"))
            .or(col("players").equalTo("Up to 20"))
            .or(col("players").equalTo("Up to 22"))
            .or(col("players").equalTo("Up to 24"))
            .or(col("players").equalTo("Up to 30"))
            .or(col("players").equalTo("Up to 32"))
            .or(col("players").equalTo("Up to 36"))
            .or(col("players").equalTo("Up to 40"))
            .or(col("players").equalTo("Up to 60"))
            .or(col("players").equalTo("Up to 64")), "11-64")
        .when(
          col("players").equalTo("1-64")
            .or(col("players").equalTo("64+"))
            .or(col("players").equalTo("64+ Online"))
            .or(col("players").equalTo("Up to more than 64"))
            .or(col("players").equalTo("Massively Multiplayer")), ">64")
        .otherwise(col("players"))
    )
    gamesDf2.show(truncate = false)

    //Añado una columna booleana que dé información sobre si el juego es Online o no. Uso expresión regular con .rlike (igual que en HIVE HQL)
    val gamesDf3 = gamesDf2.withColumn("Online Multiplayer",
      when(col("players").rlike("^(?!No Online).*Online.*")
        .or(col("maxplayers").equalTo("5-10"))
        .or(col("maxplayers").equalTo("11-64"))
        .or(col("maxplayers").equalTo(">64")), true)
        .when(
          col("players").equalTo("No Online Multiplayer")
            .or(col("maxplayers").equalTo("1"))
            .or(col("maxplayers").equalTo("2"))
            .or(col("maxplayers").equalTo("3"))
            .or(col("maxplayers").equalTo("4")), false)
        .otherwise(null))

    gamesDf3.show(25, truncate = false)
    gamesDf3.printSchema()

    //¿Cuántos videojuegos de cada género ha creado cada desarrollador
    //   Utilizo explode() y pivot() y lo hago sobre una partición:
    //   (En este dataFrame se ve que aún queda mucho trabajo por hacer de limpieza, por ejemplo hay un género llamado "online")
    val gamesDfParticion =gamesDf3
      .repartition(col("name"), col("developer"), col("genre"), col("score"), col("user score"))
    gamesDfParticion.show(9, truncate = false)

    val explodedGenre = gamesDfParticion
      .select(col("name"),
        col("developer"),
        explode(col("genre")).as("genre"),
        col("score"),
        col("user score"))

    val tablaGeneros = explodedGenre
      .groupBy(col("developer"))
      .pivot("genre")
      .agg(avg("score").as("avg score"))
      .orderBy("developer")
    tablaGeneros.show(30, truncate = false)

    //¿Cuál es el desarrollador que crea los juegos de lucha (Fighting) más valorados?
    tablaGeneros
      .select("developer", "Fighting")
      .orderBy(desc("Fighting"))
      .show(5, truncate = false)

    gamesDf3.agg(min(col("r-date"))).show()
    //Ideas para usar Window:
    //¿Cuál es el mejor juego de cada año según la puntuación de Metacritic y según los usuarios?
    val ventanayear = Window.partitionBy(year(col("r-date")))
    val bestgamesDf = gamesDf3
      .withColumn("year", year(col("r-date")))
      .withColumn("max score", max("score").over(ventanayear))
      .withColumn("max user score", max("user score").over(ventanayear))
      .filter(col("score")
        .equalTo(col("max score"))
        .or(col("user score")
          .equalTo(col("max user score"))))
      .select("year", "name", "score", "user score")
      .orderBy(col("year"), desc("score"), desc("user score"))

    bestgamesDf.show(60, truncate = false)

    //Total de usuarios para cada desarrollador y cada plataforma con una función window
    val ventanausers = Window.partitionBy("name", "platform")
    val mostplayedgamesDf = gamesDf3
      .withColumn("total users", sum("users").over(ventanausers))
      .orderBy(desc("total users"))
    mostplayedgamesDf.show(15, truncate = false)

    //Los 10 juegos en los que hay más diferencia entre la puntuación de Metacritic y la del usuario.
    val controversialgamesDf = gamesDf3
      .withColumn("year", year(col("r-date")))
      .withColumn("score difference", abs(col("score") - col("user score")))
      .select("name", "year", "score", "user score", "score difference")
      .orderBy(desc("score difference"))

    controversialgamesDf.show(10, truncate = false)


    /*
    val tablaContingencia = gamesDf3.filter(col("DESC_DISTRITO").isin("CENTRO", "BARAJAS", "RETIRO"))
      .groupBy(col("COD_EDAD_INT"))
      .pivot("DESC_DISTRITO")
      .agg(sum("espanolesmujeres"))
      .orderBy("COD_EDAD_INT")
    */



  }

}
