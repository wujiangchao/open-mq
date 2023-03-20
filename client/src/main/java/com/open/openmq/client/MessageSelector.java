

package com.open.openmq.client;


import com.open.openmq.common.filter.ExpressionType;

/**
 * Message selector: select message at server.
 * <p>
 * Now, support:
 * <li>Tag: {@link com.open.openmq.common.filter.ExpressionType#TAG}
 * </li>
 * <li>SQL92: {@link com.open.openmq.common.filter.ExpressionType#SQL92}
 * </li>
 * </p>
 */
public class MessageSelector {

    /**
     * @see com.open.openmq.common.filter.ExpressionType
     */
    private String type;

    /**
     * expression content.
     */
    private String expression;

    private MessageSelector(String type, String expression) {
        this.type = type;
        this.expression = expression;
    }

    /**
     * Use SQL92 to select message.
     *
     * @param sql if null or empty, will be treated as select all message.
     */
    public static MessageSelector bySql(String sql) {
        return new MessageSelector(ExpressionType.SQL92, sql);
    }

    /**
     * Use tag to select message.
     *
     * @param tag if null or empty or "*", will be treated as select all message.
     */
    public static MessageSelector byTag(String tag) {
        return new MessageSelector(ExpressionType.TAG, tag);
    }

    public String getExpressionType() {
        return type;
    }

    public String getExpression() {
        return expression;
    }
}
