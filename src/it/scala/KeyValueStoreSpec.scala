package com.iravid.fs2.kafka.streams

import cats.effect.{ IO, Resource }
import cats.implicits._
import com.iravid.fs2.kafka.UnitSpec
import com.iravid.fs2.kafka.client.KafkaSettings
import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{ Files, Path, SimpleFileVisitor }
import scodec._, codecs._, codecs.implicits._

case class Record(i: String, k: Int, bla: Boolean)
object Record {
  implicit val codec: Codec[Record] =
    (variableSizeBytes(uint16, utf8) :: int32 :: bool).as[Record]
}

class KeyValueStoreSpec extends UnitSpec with KafkaSettings {
  val kvStores = new RocksDBKVStores[IO]
  val testData = Record("hello", 15, true)

  def tempDir: Resource[IO, Path] =
    Resource.make(IO(Files.createTempDirectory("rocksdbtests")))(dir =>
      IO {
        Files.walkFileTree(
          dir,
          new SimpleFileVisitor[Path] {
            override def visitFile(file: Path, attrs: BasicFileAttributes) = {
              Files.delete(file)
              FileVisitResult.CONTINUE
            }

            override def postVisitDirectory(dir: Path, ex: IOException) = {
              Files.delete(dir)
              FileVisitResult.CONTINUE
            }
          }
        )

        ()
    })

  "The RocksDBKeyValueStore" must {
    implicit val key: Key.Aux[Codec, String, Record] = Key.instance

    "work properly" in {
      val storeResource: Resource[IO, KVStore[IO, String, Record]] = for {
        dir   <- tempDir
        store <- kvStores.open(dir)
      } yield store.monomorphize[String, Record]

      val (first, second, third, values) = (storeResource use { store =>
        for {
          first  <- store.get("hello")
          _      <- store.put("hello", testData)
          second <- store.get("hello")
          _      <- store.delete("hello")
          third  <- store.get("hello")
          _      <- store.put("hello", testData)
          _      <- store.put("hello2", testData)
          values <- store.scan.compile.toVector
        } yield (first, second, third, values)
      }).unsafeRunSync()

      first shouldBe empty
      second.value shouldBe testData
      third shouldBe empty
      values should contain allOf ("hello" -> testData, "hello2" -> testData)
    }

    "reopen databases properly" in {
      val program = for {
        databaseDir <- tempDir
        firstRecord <- Resource.liftF {
                        kvStores.open(databaseDir) use { firstStore =>
                          firstStore.get("hello") <*
                            firstStore.put("hello", testData)
                        }
                      }
        secondRecord <- Resource.liftF {
                         kvStores.open(databaseDir) use { secondStore =>
                           secondStore.get("hello")
                         }
                       }
      } yield (firstRecord, secondRecord)
      val (first, second) = program.use(IO(_)).unsafeRunSync

      first shouldBe empty
      second.value shouldBe testData
    }
  }
}
