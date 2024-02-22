package br.com.actionsys.kawhyimport.util;

import java.util.ArrayList;
import java.util.List;
import javax.xml.namespace.QName;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class APathUtil {

  public static int count(Node node, String aPath) throws XPathExpressionException {

    return getNodeList(node, aPath).getLength();
  }

  public static String getString(Node node, String aPath) throws XPathExpressionException {

    return (String) getValue(node, aPath, XPathConstants.STRING);
  }

  public static List<String> getStringList(Node node, String aPath)
      throws XPathExpressionException {

    NodeList nodeList = getNodeList(node, aPath);

    List<String> result = new ArrayList<>();

    for (int i = 0; i < nodeList.getLength(); i++) {
      result.add(nodeList.item(i).getTextContent());
    }

    return result;
  }

  public static NodeList getNodeList(Node node, String aPath)
      throws XPathExpressionException {

    return (NodeList) getValue(node, aPath, XPathConstants.NODESET);
  }

  private static Object getValue(Node node, String aPath, QName returnType)
      throws XPathExpressionException {

    XPath xPath = XPathFactory.newInstance().newXPath();

    if (StringUtils.startsWith(aPath, "count(")) {
      aPath = formatExpression(StringUtils.removeStart(aPath, "count("));
      aPath = "count(" + aPath;
    } else {
      aPath = formatExpression(aPath);
    }

    return xPath.compile(aPath).evaluate(node, returnType);
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
