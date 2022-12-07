package com.meta.conn.tdbank

import java.util

import com.tencent.tdw.spark.toolkit.tdbank.TubeReceiverConfig
import org.apache.spark.storage.StorageLevel

/**
 * TDbank 连接配置类
 *
 * @author weitaoliang
 */
class TDBanReceiverConfig extends TubeReceiverConfig {
  private var _master: String = _
  private var _topic: String = _
  private var _group: String = _
  private var _consumeFromMaxOffset: Boolean = true
  private var _filterOnRemote: Boolean = true
  private var _storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  // 指定消费的接口id，null表示消费topic下所有的tid/iname,oceanus写入的为iNames，其他一般是tids
  private var _tids: Array[String] = _

  override def master: String = _master

  override def setMaster(value: String): TDBanReceiverConfig.this.type = {
    _master = value
    this
  }

  override def topic: String = _topic

  override def setTopic(value: String): TDBanReceiverConfig.this.type = {
    _topic = value
    this
  }

  override def group: String = _group

  override def setGroup(value: String): TDBanReceiverConfig.this.type = {
    _group = value
    this
  }

  def tids: Array[String] = _tids

  override def setTids(value: Array[String]): TDBanReceiverConfig.this.type = {
    _tids = value
    this
  }

  override def setConsumeFromMaxOffset(value: Boolean): TDBanReceiverConfig.this.type = {
    _consumeFromMaxOffset = value
    this
  }

  override def setFilterOnRemote(value: Boolean): TDBanReceiverConfig.this.type = {
    _filterOnRemote = value
    this
  }

  override def storageLevel: StorageLevel = _storageLevel

  override def setStorageLevel(value: StorageLevel): TDBanReceiverConfig.this.type = {
    _storageLevel = value
    this
  }

  def buildFrom(master: String,
                group: String,
                topic: String,
                tids: Array[String],
                filterOnRemote: Boolean,
                consumeFromMaxOffset: Boolean,
                storageLevel: StorageLevel): TDBanReceiverConfig.this.type = {
    this._master = master
    this._group = group
    this._topic = topic
    this._tids = tids
    this._filterOnRemote = filterOnRemote
    this._consumeFromMaxOffset = consumeFromMaxOffset
    this._storageLevel = storageLevel
    this
  }
}
