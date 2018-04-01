{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE LambdaCase #-}


-- | SQL Server database API.

module Database.ODBC.SQLServer
  ( -- * Building
    -- $building

    -- * Basic library usage
    -- $usage

    -- * Connect/disconnect
    Internal.connect
  , Internal.close
  , Internal.Connection

    -- * Executing queries
  , Internal.queryMaps
  , Internal.exec
  , query
  , SqlValue(..)
  , FromValue(..)
  , FromRow(..)
  , Internal.Binary(..)

    -- * Streaming results
    -- $streaming

  , stream
  , Internal.Step(..)

    -- * Exceptions
    -- $exceptions

  , Internal.ODBCException(..)

  ) where


import           Control.Exception
import           Control.Monad.IO.Class
import           Control.Monad.IO.Unlift
import           Data.Char
import           Data.Monoid ((<>))
import           Data.Text (Text)
import qualified Data.Text as T
import           Data.Word
import           Database.ODBC.Conversion
import           Database.ODBC.Internal (SqlValue(..), Connection)
import qualified Database.ODBC.Internal as Internal
import qualified Formatting

-- $building
--
-- You have to compile your projects using the @-threaded@ flag to
-- GHC. In your .cabal file, this would look like: @ghc-options: -threaded@

-- $usage
--
-- An example program using this library:
--
-- @
-- {-\# LANGUAGE OverloadedStrings \#-}
-- import Database.ODBC
-- main :: IO ()
-- main = do
--   conn <-
--     connect
--       "DRIVER={ODBC Driver 13 for SQL Server};SERVER=192.168.99.100;Uid=SA;Pwd=Passw0rd"
--   exec conn "DROP TABLE IF EXISTS example"
--   exec conn "CREATE TABLE example (id int, name ntext, likes_tacos bit)"
--   exec conn "INSERT INTO example VALUES (1, \'Chris\', 0), (2, \'Mary\', 1)"
--   rows <- query conn "SELECT * FROM example" :: IO [[Maybe Value]]
--   print rows
--   rows2 <- query conn "SELECT * FROM example" :: IO [(Int,Text,Bool)]
--   print rows2
--   close conn
-- @
--
-- The @rows@ list contains rows of some value that could be
-- anything. The @rows2@ list contains tuples of exactly @Int@,
-- @Text@ and @Bool@. This is achieved via the 'FromRow' class.
--
-- You need the @OverloadedStrings@ extension so that you can write
-- 'Text' values for the queries and executions.
--
-- The output of this program for @rows@:
--
-- @
-- [[Just (IntValue 1),Just (TextValue \"Chris\"),Just (BoolValue False)],[Just (IntValue 2),Just (TextValue \"Mary\"),Just (BoolValue True)]]
-- @
--
-- The output for @rows2@:
--
-- @
-- [(1,\"Chris\",False),(2,\"Mary\",True)]
-- @

-- $exceptions
--
-- Proper connection handling should guarantee that a close happens at
-- the right time. Here is a better way to write it:
--
-- @
-- {-\# LANGUAGE OverloadedStrings \#-}
-- import Control.Exception
-- import Database.ODBC.SQLServer
-- main :: IO ()
-- main =
--   bracket
--     (connect
--        "DRIVER={ODBC Driver 13 for SQL Server};SERVER=192.168.99.100;Uid=SA;Pwd=Passw0rd")
--     close
--     (\\conn -> do
--        rows <- query conn "SELECT N'Hello, World!'"
--        print rows)
-- @
--
-- If an exception occurs inside the lambda, 'bracket' ensures that
-- 'close' is called.

-- $streaming
--
-- Loading all rows of a query result can be expensive and use a lot
-- of memory. Another way to load data is by fetching one row at a
-- time, called streaming.
--
-- Here's an example of finding the longest string from a set of
-- rows. It outputs @"Hello!"@. We only work on 'Text', we ignore
-- for example the @NULL@ row.
--
-- @
-- {-\# LANGUAGE OverloadedStrings, LambdaCase \#-}
-- import qualified Data.Text as T
-- import           Control.Exception
-- import           Database.ODBC.SQLServer
-- main :: IO ()
-- main =
--   bracket
--     (connect
--        \"DRIVER={ODBC Driver 13 for SQL Server};SERVER=192.168.99.101;Uid=SA;Pwd=Passw0rd\")
--     close
--     (\\conn -> do
--        exec conn \"DROP TABLE IF EXISTS example\"
--        exec conn \"CREATE TABLE example (name ntext)\"
--        exec
--          conn
--          \"INSERT INTO example VALUES (\'foo\'),(\'bar\'),(NULL),(\'mu\'),(\'Hello!\')\"
--        longest <-
--          stream
--            conn
--            \"SELECT * FROM example\"
--            (\\longest mtext ->
--               pure
--                 (Continue
--                    (maybe
--                       longest
--                       (\\text ->
--                          if T.length text > T.length longest
--                            then text
--                            else longest)
--                       mtext)))
--            \"\"
--        print longest)
-- @

--------------------------------------------------------------------------------
-- Types

--------------------------------------------------------------------------------
-- Top-level functions

-- | Query and return a list of rows.
--
-- The @row@ type is inferred based on use or type-signature. Examples
-- might be @(Int, Text, Bool)@ for concrete types, or @[Maybe Value]@
-- if you don't know ahead of time how many columns you have and their
-- type. See the top section for example use.
query ::
     (MonadIO m, FromRow row)
  => Connection -- ^ A connection to the database.
  -> Text -- ^ SQL query.
  -> m [row]
query c txt = do
  rows <- Internal.query c txt
  case mapM fromRow rows of
    Right rows' -> pure rows'
    Left e -> liftIO (throwIO (Internal.DataRetrievalError e))



-- | Stream results like a fold with the option to stop at any time.
stream ::
     (MonadUnliftIO m, FromRow row)
  => Connection -- ^ A connection to the database.
  -> Text -- ^ SQL query.
  -> (state -> row -> m (Internal.Step state))
  -- ^ A stepping function that gets as input the current @state@ and
  -- a row, returning either a new @state@ or a final @result@.
  -> state
  -- ^ A state that you can use for the computation. Strictly
  -- evaluated each iteration.
  -> m state
  -- ^ Final result, produced by the stepper function.
stream c txt cont nil =
  Internal.stream
    c
    txt
    (\state row ->
       case fromRow row of
         Left e -> liftIO (throwIO (Internal.DataRetrievalError e))
         Right row' -> cont state row')
    nil


-- | A very conservative character escape.
escapeChar8 :: Word8 -> Text
escapeChar8 ch =
  if allowedChar (toEnum (fromIntegral ch))
     then T.singleton (toEnum (fromIntegral ch))
     else "'+CHAR(" <> Formatting.sformat Formatting.int ch <> ")+'"

-- | A very conservative character escape.
escapeChar :: Char -> Text
escapeChar ch =
  if allowedChar ch
     then T.singleton ch
     else "'+NCHAR(" <> Formatting.sformat Formatting.int (fromEnum ch) <> ")+'"

-- | Is the character allowed to be printed unescaped? We only print a
-- small subset of ASCII just for visually debugging later
-- on. Everything else is escaped.
allowedChar :: Char -> Bool
allowedChar c = (isAlphaNum c && isAscii c) || elem c (" ,.-_" :: [Char])
