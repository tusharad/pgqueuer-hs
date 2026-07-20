{-# LANGUAGE CPP #-}

module PGQueuer.Tracing (
    injectOtelContext,
    withJobSpan,
) where

import Data.Aeson (Value (..))
import PGQueuer.Types (Entrypoint (..), JobId (..))

#ifdef OTEL
import qualified Data.Aeson.Key as K
import qualified Data.Aeson.KeyMap as KM
import qualified Data.CaseInsensitive as CI
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified OpenTelemetry.Context as Context
import qualified OpenTelemetry.Context.ThreadLocal as ThreadLocal
import qualified OpenTelemetry.Propagator as Propagator
import qualified OpenTelemetry.Propagator.W3CTraceContext as W3C
import qualified OpenTelemetry.Trace.Core as Trace
import UnliftIO.Exception (catchAny, throwIO, bracket)
import Control.Monad (void)
#endif

-- | Serialize active OTel context to Aeson.Value
injectOtelContext :: IO (Maybe Value)
#ifdef OTEL
injectOtelContext = do
    mCtx <- ThreadLocal.lookupContext
    case mCtx of
        Nothing -> return Nothing
        Just ctx -> do
            let prop = W3C.w3cTraceContextPropagator
            headers <- Propagator.inject prop ctx []
            let obj = KM.fromList $ map (\(k, v) -> (K.fromText $ TE.decodeUtf8 $ CI.original k, String $ TE.decodeUtf8 v)) headers
            return $ Just (Object obj)
#else
injectOtelContext = return Nothing
#endif

-- | Wrap user execution in a new span, linking the parent context
withJobSpan :: Entrypoint -> JobId -> Maybe Value -> IO a -> IO a
#ifdef OTEL
withJobSpan (Entrypoint ep) (JobId jid) mHeaders action = do
    tp <- Trace.getGlobalTracerProvider
    let tracer = Trace.makeTracer tp "pgqueuer-hs" Trace.tracerOptions
        prop = W3C.w3cTraceContextPropagator
        headers = case mHeaders of
            Just (Object obj) ->
                [ (CI.mk $ TE.encodeUtf8 $ K.toText k, TE.encodeUtf8 s)
                | (k, String s) <- KM.toList obj
                ]
            _ -> []
    
    let emptyCtx = Context.empty
    ctx <- Propagator.extract prop headers emptyCtx
    
    let spanArgs = Trace.defaultSpanArguments 
            { Trace.attributes = HM.fromList 
                [ ("messaging.system", Trace.toAttribute ("pgqueuer" :: T.Text))
                , ("messaging.destination.name", Trace.toAttribute ep)
                , ("messaging.message.id", Trace.toAttribute (T.pack $ show jid))
                ]
            }
    
    let restore prev = case prev of
            Nothing -> void ThreadLocal.detachContext
            Just c -> void (ThreadLocal.attachContext c)

    bracket (ThreadLocal.attachContext ctx) restore $ \_ -> do
        Trace.inSpan' tracer ("process " <> ep) spanArgs $ \otelSpan -> do
            action `catchAny` \e -> do
                Trace.setStatus otelSpan (Trace.Error (T.pack $ show e))
                Trace.recordException otelSpan HM.empty Nothing e
                throwIO e
#else
withJobSpan _ _ _ action = action
#endif
