import Test.Tasty
import Test.Tasty.Runners (NumThreads (..))
import Tests.BackoffSpec (backoffTests)
import Tests.BufferSpec (bufferTests)
import Tests.CronSpec (cronTests)
import Tests.HasqlSpec (hasqlCoreTests)
import Tests.PGQueuer (pgqueuerTests)
import Tests.SimpleSpec (simpleCoreTests)
import Tests.TypesSpec (typesTests)
import Tests.WorkflowSpec (workflowTests)

main :: IO ()
main =
  defaultMain
    $ localOption (NumThreads 1)
    $ testGroup
      "PGQueuer"
      [ typesTests
      , cronTests
      , bufferTests
      , workflowTests
      , backoffTests
      , pgqueuerTests
      , testGroup
          "Core - Backend Parity"
          [ simpleCoreTests
          , hasqlCoreTests
          ]
      ]
