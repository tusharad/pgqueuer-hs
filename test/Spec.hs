import Test.Tasty
import Tests.CoreSpec (coreTests)
import Tests.PGQueuer (runTests)

main :: IO ()
main = do
    runTests
    defaultMain coreTests
