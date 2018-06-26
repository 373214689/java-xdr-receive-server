/**
 * 
 */
package com.liuyang.xdr.parser;

import com.liuyang.data.util.Row;

/**
 * @author liuyang
 *
 */
@FunctionalInterface
public interface ParserFunction {
    void accept(Row row);
}
