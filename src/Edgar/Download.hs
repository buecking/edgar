module Edgar.Download
  ( download
  , Config(..)
  , Mode(..)
  , Conditions(..)
  )
  where

import           Conduit             (MonadThrow)
import qualified Hasql.Decoders      as D
import qualified Hasql.Encoders      as E
import           Hasql.Session
import           Hasql.Statement
import           Network.HTTP.Simple

import           Edgar.Common
import           Edgar.Concurrent
import qualified Data.ByteString.Char8 as C8


download ∷ Config → IO ()
download c@Config{..} = do
    conn <- connectTo $ encodeUtf8 psql

    -- Get form filepaths
    forms <- case mode of
      QueryMode conditions →  getQueryQueue conn conditions
      IdMode    x          →  mapM (formFilename conn) x

    putStrLn $ "Requested forms: " <> show (length forms)

    forms' <- filterM (notDownloaded dir) forms

    let nToDownload = length forms'

    putStrLn $ "Not downloaded: " <> show nToDownload
    runConcurrent concurrentDLs (downloadAndSaveForm c conn dir) forms'

downloadAndSaveForm ∷ Config -> Connection → FilePath → Text → IO ()
downloadAndSaveForm c conn basedir ffn = do
    source <- downloadUrl c $ "https://www.sec.gov/Archives/" <> ffn
    createDirectoryIfMissing True $ dropFileName localPath
    writeFileLBS localPath source
  where
    localPath = basedir </> toString ffn

notDownloaded ∷ FilePath → Text → IO Bool
notDownloaded basedir ffn = not <$> doesFileExist (basedir </> toString ffn)

--------------------------------------------------------------------------------
-- Database functions                                                         --
--------------------------------------------------------------------------------
downloadUrl ∷ (MonadIO m, MonadThrow m) => Config -> Text → m LByteString
downloadUrl Config{..} url = do
    url' <- addRequestHeader "User-Agent" (C8.pack userAgent)
        <$> parseRequest (toString url)
    getResponseBody <$> (httpLBS url')

formFilename ∷ Connection → Int64 → IO Text
formFilename conn i = run (statement i formFilenameQ) conn >>= \case
    Left e → error $ show e
    Right url → return url
  where
    formFilenameQ ∷ Statement Int64 Text
    formFilenameQ = Statement sql encoder decoder True

    sql     = "select filename from forms where id = $1"
    encoder = E.param $ E.nonNullable E.int8
    decoder = D.singleRow (D.column $ D.nonNullable D.text)


getQueryQueue ∷ Connection → Conditions → IO [Text]
getQueryQueue conn cd@Conditions{..} =
    run (statement () (queryQueueQ cd)) conn >>= \case
      Left e → error $ show e
      Right r → return r
  where
    queryQueueQ ∷ Conditions → Statement () [Text]
    queryQueueQ Conditions{..} = Statement (encodeUtf8 sql) encoder decoder True

    sql       = "select filename from forms where "
                 <> (unwords . intersperse "and" . catMaybes $ conditions)
                 <> " order by random()"

    conditions = [cikCond, conameCond, typeCond, startCond, endCond, tickerCond]

    cikCond    = ("cik " <> )          <$> inConditionInt cik
    conameCond = ("company_name " <> ) <$> inConditionText companyName
    typeCond   = ("form_type " <> )    <$> inConditionText formType
    startCond  = (<>) "date_filed >= " . apostrophize True . toText . formatTime defaultTimeLocale "%F" <$> startDate
    endCond    = (<>) "date_filed <= " . apostrophize True . toText . formatTime defaultTimeLocale "%F" <$> endDate
    tickerCond = innerSELECT "cik in ( SELECT cik FROM ticker WHERE " (("symbol " <> ) <$> inConditionText ticker)

    encoder = E.noParams
    decoder = D.rowList (D.column $ D.nonNullable D.text)

innerSELECT :: Text -> Maybe Text -> Maybe Text
innerSELECT _ Nothing = Nothing
innerSELECT pref (Just x) = Just (pref <> x <> ")")


inConditionInt ∷ [Int64] → Maybe Text
inConditionInt is = inConditionGen False (map show is)

inConditionText ∷ [Text] → Maybe Text
inConditionText = inConditionGen True

inConditionGen ∷ Bool → [Text] → Maybe Text
inConditionGen _ [] = Nothing
inConditionGen addAppos as = Just . ("in " <> ) . parenthesize . unwords . intersperse "," . map (apostrophize addAppos) $ as

parenthesize ∷ Text → Text
parenthesize x  = "(" <> x <> ")"

apostrophize ∷ Bool → Text → Text
apostrophize True  x = "'" <> x <> "'"
apostrophize False x = x

--------------------------------------------------------------------------------
-- Config and CLI                                                             --
--------------------------------------------------------------------------------
data Config = Config
  { mode          ∷ !Mode
  , psql          ∷ !String
  , userAgent     ∷ !String
  , dir           ∷ !FilePath
  , concurrentDLs ∷ !Int
  }

data Mode
  = QueryMode Conditions
  | IdMode [Int64]

data Conditions = Conditions
  { cik         ∷ ![Int64]
  , ticker      ∷ ![Text]
  , companyName ∷ ![Text]
  , formType    ∷ ![Text]
  , startDate   ∷ !(Maybe Day)
  , endDate     ∷ !(Maybe Day)
  }


