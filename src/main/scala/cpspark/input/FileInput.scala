package cpspark.input

import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, TaskContext}

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.Charset

object FileInput {
  def readLines(
                 sc: SparkContext,
                 inputPath: String,
                 encoding: String,
                 minPartitions: Option[Int] = None
               ): RDD[InputLine] = {
    val files = minPartitions match {
      case Some(value) => sc.binaryFiles(inputPath, value)
      case None => sc.binaryFiles(inputPath)
    }

    files
      .filter { case (path, _) => isDataFile(path) }
      .flatMap { case (path, stream) =>
        readOneFile(
          path = path,
          stream = stream,
          encoding = encoding
        )
      }
  }

  private def readOneFile(
                           path: String,
                           stream: PortableDataStream,
                           encoding: String
                         ): Iterator[InputLine] = {
    val iterator = new FileLineIterator(
      inputId = path,
      sessionId = extractSessionId(path),
      reader = new BufferedReader(
        new InputStreamReader(
          stream.open(),
          Charset.forName(encoding)
        )
      )
    )

    val taskContext = TaskContext.get()
    if (taskContext != null) {
      taskContext.addTaskCompletionListener[Unit] { _ =>
        iterator.close()
      }
    }

    iterator
  }

  private final class FileLineIterator(
                                        inputId: String,
                                        sessionId: String,
                                        reader: BufferedReader
                                      ) extends Iterator[InputLine] with AutoCloseable {
    private var lineNo = 0L
    private var closed = false
    private var nextLine: String = readNextLine()

    override def hasNext: Boolean = {
      nextLine != null
    }

    override def next(): InputLine = {
      if (!hasNext) {
        throw new NoSuchElementException("next on empty file-line iterator")
      }

      val currentLine = nextLine
      lineNo += 1L

      val result = InputLine(
        inputId = inputId,
        sessionId = sessionId,
        lineNo = lineNo,
        line = currentLine
      )

      nextLine = readNextLine()
      result
    }

    private def readNextLine(): String = {
      try {
        val line = reader.readLine()

        if (line == null) {
          close()
        }

        line
      } catch {
        case error: Throwable =>
          close()
          throw error
      }
    }

    override def close(): Unit = {
      if (!closed) {
        closed = true
        reader.close()
      }
    }
  }

  private def isDataFile(path: String): Boolean = {
    val fileName = path.split('/').lastOption.getOrElse(path)

    !fileName.startsWith("_") &&
      !fileName.startsWith(".") &&
      fileName != "_SUCCESS"
  }

  private def extractSessionId(path: String): String = {
    val fileName = path.split('/').lastOption.getOrElse(path)

    fileName
      .stripSuffix(".log")
      .stripSuffix(".txt")
  }
}
