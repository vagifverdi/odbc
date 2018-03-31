{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings, LambdaCase #-}

-- | A helpful client for debugging connections.

import           Control.Exception
import           Data.List
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Database.ODBC.Internal as ODBC
import           System.Environment
import           System.IO
import           Text.Printf

-- | Main entry point.
main :: IO ()
main = do
  args <- getArgs
  case args of
    [connStr] -> do
      conn <- ODBC.connect (T.pack connStr)
      repl conn
    _ -> error "usage: <connection string>"

-- | Accepts a query/command and prints any results.
repl :: ODBC.Connection -> IO ()
repl c = do
  result <- prompt
  case result of
    Nothing -> pure ()
    Just input -> do
      hSetBuffering stdout LineBuffering
      catch
        (catch
           (do count <- ODBC.stream c input output (0 :: Int)
               putStrLn ("Rows: " ++ show count))
           (\case
              UserInterrupt -> pure ()
              e -> throwIO e))
        (\(e :: ODBC.ODBCException) -> putStrLn (displayException e))
      repl c
  where
    prompt = do
      hSetBuffering stdout NoBuffering
      putStr "> "
      catch (fmap Just T.getLine) (\(_ :: IOException) -> pure Nothing)
    output count row = do
      putStrLn (intercalate ", " (map showColumn row))
      pure (ODBC.Continue (count + 1))
      where
        showColumn =
          \case
            ODBC.SqlText t -> show t
            ODBC.SqlByteString bs -> show bs
            ODBC.SqlBinary bs -> show bs
            ODBC.SqlBool b -> show b
            ODBC.SqlDouble d -> printf "%f" d
            ODBC.SqlFloat d -> printf "%f" d
            ODBC.SqlInt i -> show i
            ODBC.SqlDay d -> show d
            ODBC.SqlByte b -> show b
            ODBC.SqlTimeOfDay v -> show v
            ODBC.SqlLocalTime v -> show v
            ODBC.SqlNull -> "NULL"
