package br.com.actionsys.kawhyimport.util;

import java.util.ArrayList;
import java.util.List;
import javax.xml.namespace.QName;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class APathUtil {

  public static String getStringValueFromDocument(Document document, String aPath)
      throws XPathExpressionException {

    return getString(document, aPath);
  }

  public static List<String> getStringListFromDocument(Document document, String aPath)
      throws XPathExpressionException {

    NodeList nodeList = getNodeList(document, aPath);

    List<String> result = new ArrayList<>();

    for (int i = 0; i < nodeList.getLength(); i++) {
      result.add(nodeList.item(i).getTextContent());
    }

    return result;
  }

  public static int count(Document document, String aPath) throws XPathExpressionException {
    return getNodeList(document, aPath).getLength();
  }

  private static NodeList getNodeList(Document document, String aPath)
      throws XPathExpressionException {

    return (NodeList) getValue(document, aPath, XPathConstants.NODESET);
  }

  private static String getString(Document document, String aPath) throws XPathExpressionException {

    return (String) getValue(document, aPath, XPathConstants.STRING);
  }

  private static Object getValue(Document document, String aPath, QName returnType)
      throws XPathExpressionException {

    XPath xPath = XPathFactory.newInstance().newXPath();
    return xPath.compile(formatExpression(aPath)).evaluate(document, returnType);
  }

  private static String formatExpression(String inputExpression) {

    String[] tags = inputExpression.split("/");

    StringBuilder outPutExpression = new StringBuilder();

    for (String tag : tags) {

      outPutExpression.append("/");
      if (!StringUtils.equalsAny(tag, new CharSequence[] {"*", "..", "../"})
          && !StringUtils.containsAny(tag, new CharSequence[] {"::", "@", "()", "["})) {

        if (tag.contains("|")) {
          outPutExpression.append("*[");
          String[] tagsOr = tag.split("\\|");

          for (String tagOr : tagsOr) {
            if (!tagOr.equals(tagsOr[0])) {
              outPutExpression.append(" or ");
            }
            outPutExpression.append("name()='").append(tagOr).append("'");
          }

          outPutExpression.append("]");

        } else {
          outPutExpression.append("*[name()='").append(tag).append("']");
        }

      } else {
        outPutExpression.append(tag);
      }
    }

    return outPutExpression.toString();
  }
}
