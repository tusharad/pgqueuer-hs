{-# LANGUAGE OverloadedStrings #-}

{- |
Module      : Tests.WorkflowSpec
Description : Unit tests for DAG Workflow and JobTree construction.
Copyright   : (c) Tushar Adhatrao, 2026
License     : MIT
Maintainer  : tusharadhatrao@gmail.com
Stability   : experimental
-}
module Tests.WorkflowSpec (workflowTests) where

import PGQueuer.Types (Entrypoint (..))
import PGQueuer.Workflow
import Test.Tasty
import Test.Tasty.HUnit

workflowTests :: TestTree
workflowTests =
  testGroup
    "Workflow DAG Construction"
    [ testCase "Leaf JobTree construction" $ do
        let node = JobNode (Entrypoint "leaf") Nothing 0 Nothing Nothing Nothing Nothing
            tree = node <~~ []
        case tree of
          JobTree n children -> do
            assertEqual "Entrypoint" (Entrypoint "leaf") (jnEntrypoint n)
            assertEqual "Children" 0 (length children)
    , testCase "Hierarchical JobTree with multiple children" $ do
        let parent = JobNode (Entrypoint "parent") Nothing 0 Nothing Nothing Nothing Nothing
            child1 = JobNode (Entrypoint "child1") Nothing 0 Nothing Nothing Nothing Nothing <~~ []
            child2 = JobNode (Entrypoint "child2") Nothing 0 Nothing Nothing Nothing Nothing <~~ []
            tree = parent <~~ [child1, child2]
        case tree of
          JobTree p children -> do
            assertEqual "Parent entrypoint" (Entrypoint "parent") (jnEntrypoint p)
            assertEqual "Parent child count" 2 (length children)
    , testCase "Nested multi-level JobTree" $ do
        let root = JobNode (Entrypoint "root") Nothing 0 Nothing Nothing Nothing Nothing
            mid = JobNode (Entrypoint "mid") Nothing 0 Nothing Nothing Nothing Nothing
            leaf = JobNode (Entrypoint "leaf") Nothing 0 Nothing Nothing Nothing Nothing <~~ []
            tree = root <~~ [mid <~~ [leaf]]
        case tree of
          JobTree _ [JobTree m [JobTree l _]] -> do
            assertEqual "Mid entrypoint" (Entrypoint "mid") (jnEntrypoint m)
            assertEqual "Leaf entrypoint" (Entrypoint "leaf") (jnEntrypoint l)
          _ -> assertFailure "Unexpected tree structure"
    ]
