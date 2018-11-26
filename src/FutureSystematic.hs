
{-# LANGUAGE GADTs, FlexibleInstances, UndecidableInstances #-}


module Main where

{-

Supplementary material to our PEPM'19 submission.
Some raw Haskell code.

-}

import Data.Time
import Text.Printf

import System.Mem
import System.Environment

import Data.IORef
import Data.List
import Data.Maybe
import Control.Concurrent
import Control.Concurrent.STM



cas :: Eq a => IORef a -> a -> a -> IO Bool
cas ptr old new =
   atomicModifyIORef ptr (\ cur -> if cur == old
                                   then (new, True)
                                   else (cur, False))

forkIO_ cmd = do forkIO cmd
                 return ()


-- Primitive set of (promise/future) features, specified in terms of a constructor class.
-- Inspired by Scala FP (= Scala's futures and promises).
class Core t where
  newC :: IO (t a)
  getC :: t a -> IO (Maybe a)
  tryCompleteC :: t a -> IO (Maybe a) -> IO Bool
  onCompleteC :: t a -> (Maybe a -> IO ()) -> IO ()   



-- | Atomic reference based implementation.
-- Inspired by Scala FP.
-- Invariant: Either promise not yet completed, or list of callbacks non-empty. 
-- CAS requires Eq instance, as Either predefined, define our own.
-- Via MVar () we signal if promise has been completed.
data E a b = L a | R b

data CIO a = CIO (IORef (E (Maybe a) [Maybe a -> IO ()]))
                 (MVar ())

-- | Eq required to perform CAS on atomic reference.
-- There is no need to inspect the actual values (value, list of callbacks) because:
-- 1. Once set, a promise can't be overriden.
-- 2. We only add callbacks to the list.
--
-- Requires FlexibleInstances because of the nested pattern (safe use here).
instance Eq (E (Maybe a) [Maybe a -> IO ()]) where
  (==) (L _) (R _) = False
  (==) (R _) (L _) = False
  (==) (L Nothing) (L (Just _)) = False
  (==) (L (Just _)) (L Nothing) = False
  (==) (L (Just _)) (L (Just _)) = True
  (==) (L Nothing) (L Nothing) = True
  (==) (R xs) (R ys) = length xs == length ys

instance Core CIO where
  newC =
    do x <- newIORef (R [])
       y <- newEmptyMVar
       return $ CIO x y

  getC (CIO x y) =
    do _ <- readMVar y
       (L v) <- readIORef x
       return v
 
  tryCompleteC (CIO x y) m = do
    v <- m
    let go = do
         val <- readIORef x
         case val of
           L _ -> return False
           R hs -> do b <- cas x val (L v)
                      if b
                       then do putMVar y ()
                               mapM_ (\h -> forkIO $ h v) hs
                               return True
                       else go
    go

  onCompleteC (CIO x _) h = do
       let go = do val <- readIORef x
                   case val of
                     L v -> do forkIO $ h v
                               return ()
                     R hs -> do b <- cas x val (R (h:hs))
                                if b
                                  then return ()
                                  else go
       go



-- | MVar
-- Largely similar to atomic reference impl (no spinning, blocking).
data CMVAR a = CMVAR (MVar (E (Maybe a) [Maybe a -> IO ()]))
                           (MVar ())

instance Core CMVAR where
   newC = do x <- newMVar (R [])
             y <- newEmptyMVar
             return $ CMVAR x y

   getC (CMVAR x y) = do _ <- readMVar y
                         (L v) <- readMVar x
                         return v
   tryCompleteC (CMVAR x y) m = do
        v <- m
        s <- takeMVar x
        case s of
          L _ -> do putMVar x s
                    return False
          R hs -> do putMVar x (L v)
                     putMVar y ()
                     mapM_ (\h -> forkIO $ h v) hs
                     return True

   onCompleteC (CMVAR x y) h = do
       s <- takeMVar x
       case s of
         (L v) -> do putMVar x (L v)
                     forkIO_ $ h v
         (R hs) -> putMVar x (R (h:hs))

-- | STM-based implementation
-- Invariant: Either promise not yet completed, or list of callbacks non-empty.
-- Signaling done via retry.
data CSTM a = CSTM (TVar (E (Maybe a) [Maybe a -> IO ()]))

unPrimSTM (CSTM x) = x

instance Core CSTM where
   newC = do x <- atomically $ newTVar (R []) -- newTVarIO (R [])
             return $ CSTM x

   getC (CSTM x) = atomically $ do s <- readTVar x
                                   case s of
                                     R _ -> retry
                                     L v -> return v

   tryCompleteC (CSTM x) m = do
       v <- m
       action <- atomically $ do s <- readTVar x
                                 case s of
                                    R hs -> do writeTVar x (L v)
                                               return $ do mapM_ (\h -> forkIO $ h v) hs
                                                           return True
                                    L _ -> return (return False)
       action
                                         
   onCompleteC (CSTM x) h = do
      action <- atomically $ do s <- readTVar x
                                case s of
                                  (R hs) -> do writeTVar x $ R (h:hs)
                                               return (return ())
                                  (L v) -> return (forkIO_ $ h v)
      action

                                               
-- | Futures *and* Promises.
-- We don't make any distinction at this point.
-- A future is  promise and vice versa.
-- At some later stage, we can provide a more refined interface,
-- e.g. for futures we only provide certain operations etc.

class FP t where
  new :: IO (t a)
  trySuccess :: t a -> a -> IO Bool
  tryFail :: t a -> IO Bool
  tryComplete :: t a -> IO (Maybe a) -> IO Bool
  trySuccWith :: t a -> t a -> IO ()
  tryFailWith :: t a -> t a -> IO ()
  tryCompleteWith :: t a -> t a -> IO ()

  future_ :: (() -> IO (Maybe a)) -> IO (t a)
  future :: IO (Maybe a) -> IO (t a)
  get :: t a -> IO (Maybe a)
  onComplete :: t a -> (Maybe a -> IO ()) -> IO ()
  onSuccess :: t a -> (a -> IO ()) -> IO ()
  onFail :: t a -> (() -> IO ()) -> IO ()

  transformWith :: t a -> (Maybe a -> IO (t b)) -> IO (t b)
  transform :: t a -> (Maybe a -> IO (Maybe b)) -> IO (t b)
  followedBy :: t a -> (a -> IO (Maybe b)) -> IO (t b)
  followedByWith :: t a -> (a -> IO (t b)) -> IO (t b)
  guard :: t a -> (a -> IO Bool) -> IO (t a)
  orAlt :: t a -> t a -> IO (t a)
  first :: t a -> t a -> IO (t a)
  firstSucc :: t a -> t a -> IO (t a)



-- Requires UndecidableInstaces as the context constraint Core t is no smaller than the instead head FP t.
-- For our uses, the instance declaration is safe.
-- Requires FlexibleInstances because in instead head FP t, t is a plain variable (safe use here).
instance Core t => FP t where
    new = newC
    trySuccess p x = tryCompleteC p (return $ Just x)
    tryFail p = tryCompleteC p (return Nothing)
    tryComplete = tryCompleteC
    trySuccWith p f =
        onSuccess f (\x -> do trySuccess p x
                              return ())
    tryFailWith p f = 
        onFail f (\() -> do tryFail p
                            return ())
    tryCompleteWith p f = do
        onComplete f (\x -> do tryComplete p (return x)
                               return ())
{-
-- creates 2 instead of 1 callback
        trySuccWith p f
        tryFailWith p f
-}

    future_ h = do p <- newC
                   forkIO $ do tryComplete p (h ())
                               return ()
                   return p
    future f = future_ (\() -> f)
    get = getC
    onComplete f h = onCompleteC f h
    onSuccess f h = onComplete  f (\x -> case x of
                                           Nothing -> return ()
                                           Just v -> h v)
    onFail f h = onComplete  f (\x -> case x of
                                           Nothing -> h ()
                                           Just v -> return ())

    transformWith f h = do
        p <- new
        onComplete  f (\x -> do v <- h x
                                tryCompleteWith p v)
        return p
    transform f h = do
        p <- new
        onComplete  f (\x -> do tryComplete p (h x)
                                return ())
        return p
{-

-- 'with' introduces additional callback that might lead to some run-time overhead

       transformWith f (\x -> do p <- newP
                                 tryComplete p (h x)
                                 return p)
-}
    followedBy f h =
       transform f (\x -> case x of
                           Just v -> h v
                           Nothing -> return Nothing)
{-
       followedByWith f (\x -> do p <- newP
                                  tryComplete p (h x)
                                  return p)
-}

    guard f h = followedBy f (\x -> do v <- h x
                                       if v
                                          then return $ Just x
                                          else return Nothing)    
    followedByWith f h = do
        transformWith f (\x -> case x of
                                 Just v -> h v
                                 Nothing -> do p <- new
                                               tryFail p
                                               return p)
    orAlt f1 f2 = do
      transformWith f1
        (\x -> case x of
                 Just v -> return f1
                 Nothing -> transform f2
                               (\x -> case x of
                                        Just v -> return x
                                        Nothing -> return Nothing))
{-
    orAlt f1 f2 = do
      transformWith f1
        (\x -> case x of
                 Just v -> return f1
                 Nothing -> transformWith f2
                               (\x -> case x of
                                        Just v -> return f2
                                        Nothing -> return f1))
-}

    first f1 f2 = do
      p <- new
      tryCompleteWith p f1
      tryCompleteWith p f2
      return p
    firstSucc f1 f2 = do
      p <- new
      trySuccWith p f1
      trySuccWith p f2
      return p


-- Just playing. Alternative definition.

{-
    followedBy f h = do p <- newP
                        onComplete  f (\x -> case x of
                                               Just v -> do tryCompleteP p (h v)
                                                            return ()
                                               Nothing -> do tryCompleteP p (return Nothing)
                                                             return ())
                        return p
-}

{-
    followedByWith f h = do
        p <- newP
        onComplete  f (\x -> case x of
                              Just v -> do f' <- h v
                                           tryCompleteWith p f'
                              Nothing -> do tryCompleteP p (return Nothing)
                                            return ())
        return p
-}

{-
    orAlt f1 f2 = do
      future (\() -> do v <- get f1
                        case v of
                           Just{} -> return v
                           Nothing -> get f2)
-}

{-
    orAlt f1 f2 = do
       p <- newP
       onComplete f1
         (\x -> case x of
                  Just v -> do trySuccess p v
                               return ()
                  Nothing -> onComplete f2 (\x -> case x of
                                                    Just v -> do trySuccess p v
                                                                 return ()
                                                    Nothing -> do tryFail p
                                                                  return ()))
       return p
-}


{-
    
    firstSucc f1 f2 = do
      f3 <- orAlt f1 f2
      f4 <- orAlt f2 f1
      first f3 f3
-}


-- Benchmarks

bio :: CIO a
bio = undefined

bstm :: CSTM a
bstm = undefined

bmvar :: CMVAR a
bmvar = undefined

-- Measures high/low contention
-- q to select candidate (bio, bmvar, bstm)
-- n number of promises
-- m * n number of oncomplete runs
-- k * n number of try completes
-- For each promise pi taken from [p1,...,pn]
--     m times onComplete pi incCount
--     k times tryComplete pi v
--     get pi
-- wait for counter to reach n*m (all onCompletes are processed)
perf1 n m k q = do
         x <- newTVarIO 0
         ps <- mapM (\_ -> newC) [1..n]
         let qs = q:ps -- trick to force type
         let ps' = ps
         let cmd = mapM_ (\p-> onCompleteC p (\_ -> do atomically $ do v <- readTVar x
                                                                       writeTVar x (v+1)
                                                       -- putStrLn "doo"
                                             ))
                                    ps'
         mapM_ (\_ -> forkIO cmd) [1..m]

         let ts = mapM_ (\p -> tryCompleteC p (return $ Just 1)) ps

         mapM_ (\_ -> forkIO ts) [1..k]

         mapM_ getC ps  -- XX
         atomically $ do v <- readTVar x
                         if v < n*m
                          then retry
                          else return ()




-- chain of (nested) oncomplete and tries
--  onComplete p1 (do tryComplete p2
--                    onComplete p2 (do tryComplete p3
--                                      ...            ))
--  tryComplete p1
-- get pn
perf2 n q = do
            ps <- mapM (\_ -> newC) [1..n]
            let qs = q:ps -- trick to force type
            let go (p1:p2:ps) = onCompleteC p1 (\_ -> do tryCompleteC p2 (return $ Just 1)
                                                         go (p2:ps))
                go [p] = onCompleteC p (\_ -> return ())
            go ps
            tryCompleteC (head ps) (return $ Just 1)
            _ <- getC (last ps)
            return ()


-- chain of oncomplete and tries
-- no excessive nesting
-- do onComplete p1 (tryComplete p2)
--    onComplete p2 (tryComplete p3)
--    ...
---   tryComplete p1
perf3 n q = do
            ps <- mapM (\_ -> newC) [1..n]
            let qs = q:ps -- trick to force type
            let go (p1:p2:ps) = do onCompleteC p1 (\_ -> do do tryCompleteC p2 (return $ Just 1)
                                                               return ())
                                   go (p2:ps)
                go [p] = onCompleteC p (\_ -> return ())
            go ps
            tryCompleteC (head ps) (return $ Just 1)
            _ <- getC (last ps)
            return ()


execTest test = do performGC
                   start <- getCurrentTime
                   test
                   fin <- getCurrentTime
                   let result = diffUTCTime fin start
                   printf "time: %.2fs\n" (realToFrac result :: Double)
                   return (realToFrac result :: Double)


runTest n test = do rs <- mapM (\_ -> execTest test) [1..n]
                    let xs = sort rs
                    let low = head xs
                    let high = last xs
                    let m :: Double
                        m = fromIntegral (length xs)
                    let av = sum xs / m
                    printf "low: %.2fs high: %.2fs avrg: %.2fs\n" low high av


runAll n t1 t2 t3 = do
                   putStrLn "CIO"
                   runTest n t1
                   putStrLn "CMVAR"
                   runTest n t2
                   putStrLn "CSTM"
                   runTest n t3


test1 = runAll 10 (perf1 10000 100 200 bio)
                 (perf1 10000 100 200 bmvar)
                 (perf1 10000 100 200 bstm)

test2 = runAll 10 (perf1 100000 20 2 bio)
                 (perf1 100000 20 2 bmvar)
                 (perf1 100000 20 2 bstm)

test3 = do let n = 2000000
           runAll 10 (perf2 n bio)
                     (perf2 n bmvar)
                     (perf2 n bstm)

test4 = do let n = 2000000
           runAll 10 (perf3 n bio)
                     (perf3 n bmvar)
                     (perf3 n bstm)

main = do args <- getArgs
          let t = head args
          case t of
            "1" -> do print "Test 1"
                      test1
            "2" -> do print "Test 2"
                      test2
            "3" -> do print "Test 3"
                      test3
            "4" -> do print "Test 4"
                      test4
            



