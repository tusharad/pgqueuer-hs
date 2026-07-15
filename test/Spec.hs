import Test.Tasty
import Test.Tasty.Runners (NumThreads (..))
import Tests.HasqlSpec (hasqlCoreTests)
import Tests.PGQueuer (pgqueuerTests)
import Tests.SimpleSpec (simpleCoreTests)

main :: IO ()
main =
    defaultMain $
        localOption (NumThreads 1) $
            testGroup
                "PGQueuer"
                [ pgqueuerTests
                , testGroup
                    "Core - Backend Parity"
                    [ simpleCoreTests
                    , hasqlCoreTests
                    ]
                ]
