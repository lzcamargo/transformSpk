//filter transformations from graphframes(as Helpers) and Transformation Rules
// This code was used as base for the Transformation executions in spark-shell on local mode. 
==================== Family2Person Transformation===================================
spark.conf.set("spark.sql.crossJoin.enabled", true)

val edgesDF = gf.edges.withColumn("key", regexp_extract($"key","""[:A-Za-z0-9]*$""",0))

//Filter operations from GraphFrame
val lastNameFamily = edgesDF.filter("key = 'lastName'").select($"src", $"dst", $"key")
val maleFamilyDF = lastNameFamily.select($"src".alias("srcm"), $"dst".alias("dstm")).join(edgesDF).filter($"src" === $"srcm" && ($"key" === "father" || $"key" === "sons")).drop("srcm")
val femaleFamilyDF = lastNameFamily.select($"src".alias("srcf"), $"dst".alias("dstf")).join(edgesDF).filter($"src" === $"srcf" && ($"key" === "mother" || $"key" === "daughters")).drop("srcf")

val firstNameMale = maleFamilyDF.select($"dstm",$"dst".alias("dstf")).join(edgesDF).where($"dstf" === $"src" && $"key"==="firstName").select($"dstm",$"dst")
val firstNameFemale = femaleFamilyDF.select($"dstf",$"dst".alias("dstm")).join(edgesDF).where($"dstm" === $"src" && $"key"==="firstName").select($"dstf",$"dst")

//Links among sons, daugthers, and Family Names
val sonsNamesLinks = maleFamilyDF.where($"key"==="sons").select($"dstm",$"dst".alias("dstt")).join(edgesDF).filter($"dstt"===$"src").select($"dstm", $"dst")
val daughtersNamesLinks = femaleFamilyDF.where($"key"==="daughters").select($"dstf",$"dst".alias("dstt")).join(edgesDF).filter($"dstt"===$"src").select($"dstf", $"dst")

val maleVerticeNamesDF = firstNameMale.union(sonsNamesLinks.select($"dstm",$"dst".alias("dsts")).join(edgesDF).filter($"dsts" ===$"src" && $"key"==="firstName").select($"dstm",$"dst"))
val femaleVerticeNamesDF = firstNameFemale.union(daughtersNamesLinks.select($"dstf",$"dst".alias("dsts")).join(edgesDF).filter($"dsts" ===$"src" && $"key"==="firstName").select($"dstf",$"dst"))

// Family2Person Rule
object Family2Person {
	def main(args: Array[String]): Unit = {
		val maleFullNamesDF = maleVerticeNamesDF
			.select($"dstm", $"dst").join(gf.vertices)
			.filter($"dstm" === $"id").select($"value".alias("lastName"), $"dst")
			.join(gf.vertices).filter($"dst"===$"id")
			.select(concat($"lastName", lit(" "), $"value") as "fullName")

		val femaleFullNamesDF = femaleVerticeNamesDF
			.select($"dstf", $"dst").join(gf.vertices)
			.filter($"dstf" === $"id").select($"value".alias("lastName"), $"dst")
			.join(gf.vertices).filter($"dst"===$"id")
			.select(concat($"lastName", lit(" "), $"value") as "fullName")
	}
}

val fullNamesDF = maleFullNamesDF.select("*").union(femaleFullNamesDF).select("*").repartition(1)
fullNamesDF.rdd.saveAsTextFile("../outputTransf/person")

=========================Family2Person transformation using Motifs===========================================
val subEdgesmDF = gfsubm.edges.withColumn("key", regexp_extract($"key","""[:A-Za-z0-9]*$""",0))
val subEdgesfDF = gfsubf.edges.withColumn("key", regexp_extract($"key","""[:A-Za-z0-9]*$""",0))

val lastNameVert = subEdgesmDF.where($"key"==="lastName").select($"dst", $"src")
val firstNameMVert = lastNameVert.select($"dst".as("dstn"), $"src".as("srcl")).join(subEdgesmDF).where($"srcl"===$"src" && $"key" =!= "lastName").select($"dstn",$"dst".as("dstl")).join(subEdgesmDF).where($"dstl"===$"src").select($"dstn",$"dst", $"key")
val firstNameMVertAux = firstNameMVert.where($"key" =!= "firstName").select($"dstn",$"dst".as("dstf")).join(subEdgesmDF).where($"dstf"===$"src").select($"dstn", $"src",$"dst")
val maleVerticeNamesDF = firstNameMVert.where($"key"==="firstName").select($"dstn", $"dst").union(firstNameMVertAux.select($"dstn", $"dst"))

val firstNameFVert = lastNameVert.select($"dst".as("dstn"), $"src".as("srcl")).join(subEdgesfDF).where($"srcl"===$"src" && $"key" =!= "lastName").select($"dstn",$"dst".as("dstl")).join(subEdgesfDF).where($"dstl"===$"src").select($"dstn",$"dst", $"key")
val firstNameFVertAux = firstNameFVert.where($"key" =!= "firstName").select($"dstn",$"dst".as("dstf")).join(subEdgesfDF).where($"dstf"===$"src").select($"dstn", $"src",$"dst")
val femaleVerticeNamesDF = firstNameFVert.where($"key"==="firstName").select($"dstn", $"dst").union(firstNameFVertAux.select($"dstn", $"dst"))

// Family2Person Rule
object Family2Person {
	def main(args: Array[String]): Unit = {
		val maleFullNamesDF = maleVerticeNamesDF
			.select($"dstm", $"dst").join(gf.vertices)
			.filter($"dstm" === $"id").select($"value".alias("lastName"), $"dst")
			.join(gf.vertices).filter($"dst"===$"id")
			.select(concat($"lastName", lit(" "), $"value") as "fullName")

		val femaleFullNamesDF = femaleVerticeNamesDF
			.select($"dstf", $"dst").join(gf.vertices)
			.filter($"dstf" === $"id").select($"value".alias("lastName"), $"dst")
			.join(gf.vertices).filter($"dst"===$"id")
			.select(concat($"lastName", lit(" "), $"value") as "fullName")
	}
}

val fullNamesDF = maleFullNamesDF.select("*").union(femaleFullNamesDF).select("*").toDF() //coalesce() repartition()
fullNamesDF.rdd.saveAsTextFile("../outputTransf/Persons")

===============================Family2Person transformation Using Clustering==========================================
val lastNameFamily = clusterInput.select($"node", $"cluster").join(edgesDF).where($"node" === $"dst" && $"key"==="lastName").select($"src", $"dst", $"key",$"cluster").repartition($"cluster")
val maleFamilyDF = lastNameFamily.select($"src".alias("srcm"), $"dst".alias("dstm"), $"cluster").join(edgesDF).filter($"src" === $"srcm" && ($"key" === "father" || $"key" === "sons")).drop("srcm").repartition($"cluster")
val femaleFamilyDF = lastNameFamily.select($"src".alias("srcf"), $"dst".alias("dstf"),$"cluster").join(edgesDF).filter($"src" === $"srcf" && ($"key" === "mother" || $"key" === "daughters")).drop("srcf").repartition($"cluster")

val firstNameMale = maleFamilyDF.select($"dstm",$"dst".alias("dstf"),$"cluster").join(edgesDF).where($"dstf" === $"src" && $"key"==="firstName").select($"dstm",$"dst", $"cluster").repartition($"cluster")
val firstNameFemale = femaleFamilyDF.select($"dstf",$"dst".alias("dstm"),$"cluster").join(edgesDF).where($"dstm" === $"src" && $"key"==="firstName").select($"dstf",$"dst",$"cluster").repartition($"cluster")

val sonsNamesLinks = maleFamilyDF.where($"key"==="sons").select($"dstm",$"dst".alias("dstt"),$"cluster").join(edgesDF).filter($"dstt"===$"src").select($"dstm", $"dst",$"cluster")
val daugthersNamesLinks = femaleFamilyDF.where($"key"==="daugthers").select($"dstf",$"dst".alias("dstt"),$"cluster").join(edgesDF).filter($"dstt"===$"src").select($"dstf", $"dst",$"cluster")

object Family2Person {
	def main(args: Array[String]): Unit = {
		val maleFullNamesDF = maleVerticeNamesDF
			.select($"dstm", $"dst").join(gf.vertices)
			.filter($"dstm" === $"id").select($"value".alias("lastName"), $"dst")
			.join(gf.vertices).filter($"dst"===$"id")
			.select(concat($"lastName", lit(" "), $"value") as "fullName")

		val femaleFullNamesDF = femaleVerticeNamesDF
			.select($"dstf", $"dst").join(gf.vertices)
			.filter($"dstf" === $"id").select($"value".alias("lastName"), $"dst")
			.join(gf.vertices).filter($"dst"===$"id")
			.select(concat($"lastName", lit(" "), $"value") as "fullName")
	}
}

val fullNamesDF = maleFullNamesDF.select("*").union(femaleFullNamesDF).select("*").toDF() //coalesce() repartition()
fullNamesDF.rdd.saveAsTextFile("../outputTransf/Persons")
