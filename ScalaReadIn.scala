object ScalaReadIn {
	import scala.io.Source

	def main(args: Array[String]): Unit = {
		val inputFile = "inputFile.txt"
		val inputSource = Source.fromFile(inputFile)
		while(inputSource.hasNext){
			print(inputSource.next)
		}
		inputSource.close()
	}
}
