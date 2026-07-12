module Main (main) where

import System.Environment (getArgs)

import qualified HPCScheduler
import qualified Python
import qualified Simple

main :: IO ()
main = do
    args <- getArgs
    case args of
        ["simple"] -> Simple.runApp
        ["hpcscheduler"] -> HPCScheduler.runApp
        ["python"] -> Python.runApp
        _ -> putStrLn "usage: example-exe [simple|hpcscheduler|python]"
