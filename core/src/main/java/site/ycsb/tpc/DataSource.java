package site.ycsb.tpc;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class DataSource {
  private static final Random rand = new Random();
  private static final String SQL_TIMESTAMP_STRUCT = "YYYY-MM-DDThh:mm:ss";

  private static final Nation[] nations = {
      new Nation(48, "ALGERIA", 0), new Nation(49, "ARGENTINA", 1), new Nation(50, "BRAZIL", 1),
      new Nation(51, "CANADA", 1), new Nation(52, "EGYPT", 4), new Nation(53, "ETHIOPIA", 0),
      new Nation(54, "FRANCE", 3), new Nation(55, "GERMANY", 3), new Nation(56, "INDIA", 2),
      new Nation(57, "INDONESIA", 2), new Nation(65, "IRAN", 4), new Nation(66, "IRAQ", 4),
      new Nation(67, "JAPAN", 2), new Nation(68, "JORDAN", 4), new Nation(69, "KENYA", 0),
      new Nation(70, "MOROCCO", 0), new Nation(71, "MOZAMBIQUE", 0), new Nation(72, "PERU", 1),
      new Nation(73, "CHINA", 2), new Nation(74, "ROMANIA", 3), new Nation(75, "SAUDI ARABIA", 4),
      new Nation(76, "VIETNAM", 2), new Nation(77, "RUSSIA", 3), new Nation(78, "UNITED KINGDOM", 3),
      new Nation(79, "UNITED STATES", 1), new Nation(80, "CHINA", 2), new Nation(81, "PAKISTAN", 2),
      new Nation(82, "BANGLADESH", 2), new Nation(83, "MEXICO", 1), new Nation(84, "PHILIPPINES", 2),
      new Nation(85, "THAILAND", 2), new Nation(86, "ITALY", 3), new Nation(87, "SOUTH AFRICA", 0),
      new Nation(88, "SOUTH KOREA", 2), new Nation(89, "COLOMBIA", 1), new Nation(90, "SPAIN", 3),
      new Nation(97, "UKRAINE", 3), new Nation(98, "POLAND", 3), new Nation(99, "SUDAN", 0),
      new Nation(100, "UZBEKISTAN", 2), new Nation(101, "MALAYSIA", 2), new Nation(102, "VENEZUELA", 1),
      new Nation(103, "NEPAL", 2), new Nation(104, "AFGHANISTAN", 2), new Nation(105, "NORTH KOREA", 2),
      new Nation(106, "TAIWAN", 2), new Nation(107, "GHANA", 0), new Nation(108, "IVORY COAST", 0),
      new Nation(109, "SYRIA", 4), new Nation(110, "MADAGASCAR", 0), new Nation(111, "CAMEROON", 0),
      new Nation(112, "SRI LANKA", 2), new Nation(113, "ROMANIA", 3), new Nation(114, "NETHERLANDS", 3),
      new Nation(115, "CAMBODIA", 2), new Nation(116, "BELGIUM", 3), new Nation(117, "GREECE", 3),
      new Nation(118, "PORTUGAL", 3), new Nation(119, "ISRAEL", 4), new Nation(120, "FINLAND", 3),
      new Nation(121, "SINGAPORE", 2), new Nation(122, "NORWAY", 3)
  };

  private static final String[] regions = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};

  private static final String[] typeSize = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
  private static final String[] typeAdjective = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
  private static final String[] typeMetal = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};

  private static final String[] containerSize = {"SM", "LG", "MED", "JUMBO", "WRAP"};
  private static final String[] containerType = {"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"};

  private static final String[] segments = {"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"};

  private static final String[] priorities = {"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"};

  private static final String[] instructions = {"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"};

  private static final String[] modes = {"REG AIR", "AIR RAIL", "SHIP", "TRUCK", "MAIL", "FOB"};

  private static final String[] tpchNouns = {
      "foxes", "ideas", "theodolites", "pinto beans", "instructions", "dependencies", "excuses",
      "platelets", "asymptotes", "courts", "dolphins", "multipliers", "sauternes", "warthogs",
      "frets", "dinos", "attainments", "somas", "Tiresias'", "patterns", "forges", "braids",
      "hockey players", "frays", "warhorses", "dugouts", "notornis", "epitaphs", "pearls",
      "tithes", "waters", "orbits", "gifts", "sheaves", "depths", "sentiments", "decoys",
      "realms", "pains", "grouches", "escapades"
  };

  private static final String[] tpchVerbs = {
      "sleep", "wake", "are", "cajole", "haggle", "nag", "use", "boost", "affix", "detect",
      "integrate", "maintain", "nod", "was", "lose", "sublate", "solve", "thrash", "promise",
      "engage", "hinder", "print", "x-ray", "breach", "eat", "grow", "impress", "mold",
      "poach", "serve", "run", "dazzle", "snooze", "doze", "unwind", "kindle", "play", "hang",
      "believe", "doubt"
  };

  private static final String[] tpchAdjectives = {
      "furious", "sly", "careful", "blithe", "quick", "fluffy", "slow", "quiet", "ruthless",
      "thin", "close", "dogged", "daring", "brave", "stealthy", "permanent", "enticing", "idle",
      "busy", "regular", "final", "ironic", "even", "bold", "silent"
  };

  private static final String[] tpchAdverbs = {
      "sometimes", "always", "never", "furiously", "slyly", "carefully", "blithely", "quickly",
      "fluffily", "slowly", "quietly", "ruthlessly", "thinly", "closely", "doggedly", "daringly",
      "bravely", "stealthily", "permanently", "enticingly", "idly", "busily", "regularly",
      "finally", "ironically", "evenly", "boldly", "silently"
  };

  private static final String[] tpchPrepositions = {
      "about", "above", "according to", "across", "after", "against", "along", "alongside of",
      "among", "around", "at", "atop", "before", "behind", "beneath", "beside", "besides",
      "between", "beyond", "by", "despite", "during", "except", "for", "from", "in place of",
      "inside", "instead of", "into", "near", "of", "on", "outside", "over", "past", "since",
      "through", "throughout", "to", "toward", "under", "until", "up", "upon", "without",
      "with", "within"
  };

  private static final String[] tpchTerminators = {".", ";", ":", "?", "!", "--"};

  private static final String[] tpchAuxiliaries = {
      "do", "may", "might", "shall", "will", "would", "can", "could", "should", "ought to",
      "must", "will have to", "shall have to", "could have to", "should have to", "must have to",
      "need to", "try to"
  };

  private static final String[] lastNameParts = {
      "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE",
      "ANTI", "CALLY", "ATION", "EING"
  };

  private static final int C_255 = randomUniformInt(0, 255);
  private static final int C_1023 = randomUniformInt(0, 1023);
  private static final int C_8191 = randomUniformInt(0, 8191);

  private static final int lastOlCount = 0;

  public static String tpchText(int length) {
    StringBuilder s = new StringBuilder();
    int i = 0;
    while (s.length() < length) {
      s.append(i++ == 0 ? "" : " ").append(tpchSentence());
    }
    return s.toString();
  }

  public static String tpchSentence() {
    switch (randomUniformInt(0, 4)) {
      case 0:
        return tpchNounPhrase() + " " + tpchVerbPhrase() + " " + tpchTerminator();
      case 1:
        return tpchNounPhrase() + " " + tpchVerbPhrase() + " " + tpchPrepositionalPhrase() + " " + tpchTerminator();
      case 2:
        return tpchNounPhrase() + " " + tpchVerbPhrase() + " " + tpchNounPhrase() + " " + tpchTerminator();
      case 3:
        return tpchNounPhrase() + " " + tpchPrepositionalPhrase() + " " + tpchVerbPhrase() + " "
            + tpchNounPhrase() + " " + tpchTerminator();
      default:
        return tpchNounPhrase() + " " + tpchPrepositionalPhrase() + " " + tpchVerbPhrase() + " " +
            tpchPrepositionalPhrase() + " " + tpchTerminator();
    }
  }

  public static String tpchNounPhrase() {
    switch (randomUniformInt(0, 3)) {
      case 0:
        return tpchNoun();
      case 1:
        return tpchAdjective() + " " + tpchNoun();
      case 2:
        return tpchAdjective() + ", " + tpchAdjective() + " " + tpchNoun();
      default:
        return tpchAdverb() + " " + tpchAdjective() + " " + tpchNoun();
    }
  }

  public static String tpchVerbPhrase() {
    switch (randomUniformInt(0, 3)) {
      case 0:
        return tpchVerb();
      case 1:
        return tpchAuxiliary() + " " + tpchVerb();
      case 2:
        return tpchVerb() + " " + tpchAdverb();
      default:
        return tpchAuxiliary() + " " + tpchVerb() + " " + tpchAdverb();
    }
  }

  public static String tpchPrepositionalPhrase() {
    return tpchPrepositions[randomArrayIndex(tpchPrepositions.length)] + " the " + tpchNounPhrase();
  }

  public static String tpchNoun() {
    return tpchNouns[randomArrayIndex(tpchNouns.length)];
  }

  public static String tpchVerb() {
    return tpchVerbs[randomArrayIndex(tpchVerbs.length)];
  }

  public static String tpchAdverb() {
    return tpchAdverbs[randomArrayIndex(tpchAdverbs.length)];
  }

  public static String tpchAdjective() {
    return tpchAdjectives[randomArrayIndex(tpchAdjectives.length)];
  }

  public static String tpchAuxiliary() {
    return tpchAuxiliaries[randomArrayIndex(tpchAuxiliaries.length)];
  }

  public static String tpchTerminator() {
    return tpchTerminators[randomArrayIndex(tpchTerminators.length)];
  }

  public static boolean randomTrue(double probability) {
    double value = rand.nextDouble();
    return value < probability;
  }

  public static int randomUniformInt(int minValue, int maxValue) {
    return rand.nextInt((maxValue - minValue) + 1) + minValue;
  }

  public static int randomArrayIndex(int length) {
    int minValue = 0;
    int maxValue = length - 1;
    return rand.nextInt((maxValue - minValue) + 1) + minValue;
  }

  public static double randomDouble(double minValue, double maxValue, int places) {
    double randomValue = minValue + (maxValue - minValue) * rand.nextDouble();
    return round(randomValue, places);
  }

  public static int randomNonUniformInt(int A, int x, int y) {
    int C;
    switch (A) {
      case 255:
        C = C_255;
        break;
      case 1023:
        C = C_1023;
        break;
      case 8191:
        C = C_8191;
        break;
      default:
        throw new RuntimeException("randomNonUniformInt: unexpected value for A (%d)\n" + A);
    }
    return ((((randomUniformInt(0, A) | randomUniformInt(x, y)) + C) % (y - x + 1)) + x);
  }

  static String lastName(int num) {
    String name;

    name = lastNameParts[num / 100];
    name = name + lastNameParts[(num / 10) % 10];
    name = name + lastNameParts[num % 10];

    return name;
  }

  public static double round(double value, int places) {
    BigDecimal bd = BigDecimal.valueOf(value);
    bd = bd.setScale(places, RoundingMode.HALF_UP);
    return bd.doubleValue();
  }

  public static int permute(int value, int low, int high) {
    int range = high - low + 1;
    return ((value * 9973) % range) + low;
  }

  public static Date getCurrentTimestamp() {
    return new Date();
  }

  public static String getType() {
    return typeSize[randomUniformInt(0, typeSize.length - 1)] + " " +
        typeAdjective[randomUniformInt(0, typeAdjective.length - 1)] + " " +
        typeMetal[randomUniformInt(0, typeMetal.length - 1)];
  }

  public static String addNumericString(int length) {
    StringBuilder s = new StringBuilder();
    for (int i = 0; i < length; i++) {
      s.append(randomUniformInt(0, 9));
    }
    return s.toString();
  }

  public static String newAlphanumericString64(int length) {
    Base64.Encoder encoder = Base64.getUrlEncoder();
    StringBuilder s = new StringBuilder();
    while (s.length() < length) {
      UUID uuid = UUID.randomUUID();
      s.append(encoder.encodeToString(uuid.toString().getBytes()));
    }
    return s.substring(0, length);
  }

  public static String newAlphanumericString62(int minValue, int maxValue) {
    String numbers = "0123456789";
    String upper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    String lower = "abcdefghijklmnopqrstuvwxyz";
    String characters = upper + lower + numbers;
    char[] alphanum = characters.toCharArray();
    int length = randomUniformInt(minValue, maxValue);
    int max = alphanum.length - 1;

    StringBuilder string = new StringBuilder();
    for (int i = 0; i < length; i++) {
      string.append(alphanum[randomUniformInt(0, max)]);
    }

    return string.toString();
  }

  public static String newNumericString62(int minValue, int maxValue) {
    String numbers = "0123456789";
    char[] alphanum = numbers.toCharArray();
    int length = randomUniformInt(minValue, maxValue);
    int max = alphanum.length - 1;

    StringBuilder string = new StringBuilder();
    for (int i = 0; i < length; i++) {
      string.append(alphanum[randomUniformInt(0, max)]);
    }

    return string.toString();
  }

  public static String addTextString(int minLength, int maxLength) {
    String block = tpchText(maxLength);
    int length = randomUniformInt(minLength, maxLength);
    int offset = randomUniformInt(0, (maxLength - length - 1));
    return block.substring(offset, length);
  }

  public static String addTextStringCustomer(int minLength, int maxLength, String action) {
    StringBuilder s = new StringBuilder();
    int offset = 8 + action.length();
    int window = maxLength - minLength - offset;
    int l1 = randomUniformInt(0, window);
    int l2 = randomUniformInt(0, window - l1);
    int l3 = window - l2 - l1;
    s.append(tpchText(l1));
    s.append("Customer");
    s.append(tpchText(l2));
    s.append(action);
    s.append(tpchText(l3));
    return s.toString();
  }

  public static String addInt(int minValue, int maxValue) {
    return String.valueOf(randomUniformInt(minValue, maxValue));
  }

  public static String addDouble(double minValue, double maxValue, int places) {
    return String.valueOf(randomDouble(minValue, maxValue, places));
  }

  public static String addPhoneNumber(int nationKey) {
    return strLeadingZero(nationKey, 2) + "-" +
        randomUniformInt(100, 999) + "-" +
        randomUniformInt(100, 999) + "-" +
        randomUniformInt(1000, 9999);
  }

  public static Date getStartDate() {
    DateFormat format = new SimpleDateFormat(SQL_TIMESTAMP_STRUCT);
    try {
      return format.parse("1992-01-01T12:00:00");
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static Date getCurrentDate() {
    DateFormat format = new SimpleDateFormat(SQL_TIMESTAMP_STRUCT);
    try {
      return format.parse("1995-06-17T12:00:00");
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  public static Date getEndDate() {
    DateFormat format = new SimpleDateFormat(SQL_TIMESTAMP_STRUCT);
    try {
      return format.parse("1998-12-31T12:00:00");
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  private static Date subtractDays(int days, Date date) {
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.add(Calendar.DATE, -days);
    return c.getTime();
  }

  private static Date addDays(int days, Date date) {
    Calendar c = Calendar.getInstance();
    c.setTime(date);
    c.add(Calendar.DATE, days);
    return c.getTime();
  }

  private static Date randomDate(Date start, Date end) {
    long diff = ChronoUnit.DAYS.between(start.toInstant(), end.toInstant());
    int offset = randomUniformInt(1, (int) diff);
    Calendar c = Calendar.getInstance();
    c.setTime(start);
    c.add(Calendar.DATE, offset);
    return c.getTime();
  }

  public static String strLeadingZero(int i, int zeros) {
    return String.format("%0" + zeros + "d", i);
  }

  public static Nation getNation(int i) {
    return nations[i];
  }

  public static String getRegion(int i) {
    return regions[i];
  }

  public static class Nation {
    int id;
    String name;
    int regionId;

    public Nation(int id, String name, int regionId) {
      this.id = id;
      this.name = name;
      this.regionId = regionId;
    }
  }
}
