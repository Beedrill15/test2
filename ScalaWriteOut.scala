import java.io.File
import java.io.PrintWriter

object ScalaWriteOut {
	def main(arg:Array[String]) {
		val outputFile = new File("outputFile.txt")
		val outputWriter = new PrintWriter(outputFile)
		outputWriter.write("I have written to a file from scala.  ")
		outputWriter.close()
	}
}
