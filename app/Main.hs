import           Control.Monad.Writer       (Writer)
import           Options.Applicative
import           Options.Applicative.Simple

import           Edgar.Common
import qualified Edgar.Download             as Download
import qualified Edgar.Init                 as Init
import qualified Edgar.Update               as Update

-----------------------------------------------------------

projectVersion, projectDesc :: String
projectVersion = "v0.1.3"
projectDesc = "edgar - an archiver for SEC corporate filings data"

-----------------------------------------------------------


data Command
    = Init Init.Config
    | UpdateForm   Update.Config
    | UpdateTicker Update.Config
    | Download Download.Config

commands ∷ ExceptT Command (Writer (Mod CommandFields Command)) ()
commands = do
    addCommand     "init"     "Initialize database"        Init           initConf
    addSubCommands "update"   "Update indicies" do
      addCommand "form"   "Update master form index"   UpdateForm updateConf
      addCommand "ticker" "Update cik->ticker table"   UpdateTicker updateConf
    addSubCommands "download" "Download forms"      do
      addCommand "query" "Download documents satisfying specified conditions" Download downloadQueryMode
      addCommand "id"    "Download documents with given ids"                  Download downloadIdMode



main ∷ IO ()
main = do
    command <- snd <$> simpleOptions
                         projectVersion
                         projectDesc
                         "See --help for details"
                         (pure ())
                         commands
    case command of
      Init c          → Init.initDb c
      Download c      → Download.download c
      UpdateForm c    → Update.updateDbWithIndex c
      UpdateTicker c  → Update.updateTickerIndex c

--------------------------------------------------------------------------------
-- Command Parsers                                                            --
--------------------------------------------------------------------------------
initConf ∷ Parser Init.Config
initConf = Init.Config <$> postgres

updateConf ∷ Parser Update.Config
updateConf = Update.Config
    <$> argument auto (metavar "START"<> help "Start year quarter specified as YYYYqQ (e.g. 1999q1)")
    <*> optional (argument auto (metavar "END" <> help "End year quarter specified as YYYYqQ (OPTIONAL - Downloads only START when omitted)"))
    <*> postgres
    <*> useragent

downloadQueryMode = downloadConfig queryMode
downloadIdMode    = downloadConfig idMode

idMode ∷ Parser Download.Mode
idMode = Download.IdMode <$> many (argument auto (metavar "FORM-ID"))

queryMode ∷ Parser Download.Mode
queryMode = Download.QueryMode <$> conditions


downloadConfig ∷ Parser Download.Mode → Parser Download.Config
downloadConfig modeParser = Download.Config
    <$> modeParser
    <*> postgres
    <*> useragent
    <*> option   auto (short 'd' <> long "directory"            <> value "edgar-forms" <> showDefault <> help "Archive root directory")
    <*> option   auto (short 'n' <> long "concurrent-downloads" <> value 1   <> showDefault <> help "Number of concurrent downloads")


conditions ∷ Parser Download.Conditions
conditions = Download.Conditions
    <$> many     (option auto (short 'c' <> long "cik" <> metavar "INT" <> help "CIKs to download"))
    <*> many     (textOption  (short 't' <> long "ticker" <> metavar "TICKER" <> help "reference CIK by current ticker"))
    <*> many     (textOption  (short 'n' <> long "name" <> metavar "TEXT" <> help "Company names"))
    <*> many     (textOption  (short 'f' <> long "form-type" <> metavar "TEXT" <> help "Form types to download"))
    <*> optional (option auto (short 's' <> long "start" <> metavar "DATE" <> help "Start date (YYYY-MM-DD)"))
    <*> optional (option auto (short 'e' <> long "end" <> metavar "DATE" <> help "End date (YYYY-MM-DD)"))


--------------------------------------------------------------------------------
-- Individual option  parsers                                                 --
--------------------------------------------------------------------------------
postgres ∷ Parser String
postgres = strOption (short 'p' <> long "postgres" <> value "dbname=edgar" <> showDefault <> help "Postgres path postgresql::/username:password@host:port/database")

useragent :: Parser String
useragent = strOption
    (  long "user-agent"
    <> value (projectVersion <> " " <> projectDesc)
    <> showDefault
    <> help "Required for automated access; see https://www.sec.gov/os/accessing-edgar-data"
    )
