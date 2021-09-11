module Edgar.Common
  (
  -- * Types
    TickerCikLookup(..)
  , EdgarForm(..)
  , YearQtr
  , yearQtr
  , year
  , qtr
  -- , Day

  -- * Hasql
  , connectTo
  , Connection

  , encodeEdgarForm
  , encodeTickerCikLookup

  -- * Optparse Applicative
  , textOption

  -- * Reexported modules
  , module X

  ) where

import           Data.Time.Calendar              as X (Day)
import           Data.Time.Format                as X (defaultTimeLocale,
                                                       formatTime, parseTimeM)
import           Relude                          as X
import           System.Directory                as X
import           System.FilePath                 as X

import qualified Data.ByteString.Lazy.Char8      as L8
import           Data.Char                       (isDigit)
import           Data.Csv (FromRecord, ToRecord, FromField(..), ToField(..))
import           GHC.Read
import           Hasql.Connection
import qualified Hasql.Encoders                  as E
import qualified Options.Applicative             as Opt
import           Prelude                         (read)
import           Text.ParserCombinators.Parsec   hiding ((<|>))
import qualified Text.ParserCombinators.ReadP    as RP
import qualified Text.ParserCombinators.ReadPrec as RP


connectTo ∷ ByteString → IO Connection
connectTo b = acquire b >>= \case
    Left e  → error "Unable to connect to database"
    Right c → return c

data TickerCikLookup = TickerCikLookup
  { tickerTicker ∷ !Text  -- one CIK may link to many tickers
  , tickerCik    ∷ !Int64 -- i.e. 2020  GSX -> GOTU
                          --      2018 PTIE -> SAVA
                          -- Historical changes does not
                          -- exist in SEC's raw data, but the
                          -- information can be found in
                          -- EFFECT forms
  } deriving (Generic, Show)

instance FromRecord TickerCikLookup
instance ToRecord   TickerCikLookup

encodeTickerCikLookup :: E.Params TickerCikLookup
encodeTickerCikLookup
    = contramap tickerTicker (E.param $ E.nonNullable E.text)
   <> contramap tickerCik (E.param $ E.nonNullable E.int8)

data EdgarForm = EdgarForm
  { cik         ∷ !Int64
  , companyName ∷ !Text
  , formType    ∷ !Text
  , dateFiled   ∷ !Day
  , filename    ∷ !Text
  } deriving (Generic, Show)

instance FromRecord EdgarForm
instance ToRecord   EdgarForm

instance ToField Day where
    toField = error "Day is not intended to be converted to a CSV field"

instance FromField Day where
    parseField = parseTimeM True defaultTimeLocale "%Y-%m-%d" . L8.unpack . L8.fromStrict

encodeEdgarForm
    = contramap cik (E.param $ E.nonNullable E.int8)
   <> contramap companyName (E.param $ E.nonNullable E.text)
   <> contramap formType (E.param $ E.nonNullable E.text)
   <> contramap dateFiled (E.param $ E.nonNullable E.date)
   <> contramap filename (E.param $ E.nonNullable E.text)

textOption ∷ Opt.Mod Opt.OptionFields String → Opt.Parser Text
textOption ms = toText <$> Opt.strOption ms

data YearQtr = YearQtr
  { year ∷ !Int
  , qtr  ∷ !Int
  } deriving (Show, Eq, Ord)

yearQtr ∷ Int → Int → YearQtr
yearQtr y q =
    if | y < 1994      → error "Edgar index starts in 1994"
       | q < 1 || q > 4 → error "Quarter must be between 1 and 4"
       | otherwise     → YearQtr y q

instance Read YearQtr where
    readsPrec _ s = [(parseYq s, "")]
    readPrec  = RP.lift readYqP

parseYq ∷ String → YearQtr
parseYq s =
    case Text.ParserCombinators.Parsec.runParser parseYq' () "YearQtr" s of
      Left  e → error $ show e
      Right r → r


parseYq' ∷ Text.ParserCombinators.Parsec.Parser YearQtr
parseYq' = do
    y <- many1 digit
    char 'q' <|> char 'Q'
    q <- digit
    return $ yearQtr (read y) (read $ q : "")


readYqP ∷ RP.ReadP YearQtr
readYqP = do
    y <- RP.count 4 $ RP.satisfy isDigit
    _ <- RP.satisfy (\c → c == 'q' || c == 'Q')
    q <- RP.satisfy isDigit
    _ <- RP.eof
    return $ yearQtr (read y) (read $ q : "")

instance Enum YearQtr where
    fromEnum yq = year yq * 4 + (qtr yq - 1)
    toEnum    i = YearQtr y m
      where
        m = mod i 4 + 1
        y = quot i 4

