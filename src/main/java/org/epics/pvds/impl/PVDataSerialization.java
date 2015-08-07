package org.epics.pvds.impl;

import java.nio.ByteBuffer;

import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.pv.DeserializableControl;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVField;
import org.epics.pvdata.pv.PVScalarArray;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVStructureArray;
import org.epics.pvdata.pv.PVUnion;
import org.epics.pvdata.pv.PVUnionArray;
import org.epics.pvdata.pv.Scalar;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.SerializableControl;
import org.epics.pvdata.pv.StringArrayData;
import org.epics.pvdata.pv.StructureArrayData;
import org.epics.pvdata.pv.UnionArrayData;

public final class PVDataSerialization {

	public static final SerializableControl NOOP_SERIALIZABLE_CONTROL = new NoopSerializableControl();
	public static final DeserializableControl NOOP_DESERIALIZABLE_CONTROL = new NoopDeserializableControl();

	public static final int[] scalarSize = {
		1, // pvBoolean
		1,    // pvByte
		2,   // pvShort
		4,     // pvInt
		8,    // pvLong
		1,   // pvUByte
		2,  // pvUShort
		4,    // pvUInt
		8,   // pvULong
		4,   // pvFloat
		8,  // pvDouble
		0   // pvString
	};

	public static int getSerializationSize(PVField pvField)
	{
		Field field = pvField.getField();
		switch (field.getType())
		{
		case scalar:
		{
			ScalarType scalarType = ((Scalar)field).getScalarType();
			if (scalarType == ScalarType.pvString)
				return getStringSerializationSize(((PVString)pvField).get());
			else
				return scalarSize[scalarType.ordinal()];
		}
		case structure:
		{
			return getStructureSerializationSize((PVStructure)pvField);
		}
		case union:
		{
			return getUnionSerializationSize((PVUnion)pvField);
		}
		case scalarArray:
		{
			PVScalarArray pvScalarArray = (PVScalarArray)pvField;
			int arrayLength = pvScalarArray.getLength();
			ScalarType elementType = pvScalarArray.getScalarArray().getElementType();
			if (elementType == ScalarType.pvString)
			{
				StringArrayData sad = new StringArrayData();
				((PVStringArray)pvScalarArray).get(0, arrayLength, sad);
				int size = getSerializationSizeSize(arrayLength);
				for (String s : sad.data)
					size += getStringSerializationSize(s);
				return size;
			}
			else
				return getSerializationSizeSize(arrayLength) + arrayLength * scalarSize[elementType.ordinal()];
		}
		case structureArray:
		{
			PVStructureArray pvStructureArray = (PVStructureArray)pvField;
			int arrayLength = pvStructureArray.getLength();
			int size = getSerializationSizeSize(arrayLength);
			StructureArrayData sad = new StructureArrayData();
			pvStructureArray.get(0, arrayLength, sad);
			for (PVStructure s : sad.data)
				size += 1 + getStructureSerializationSize(s);
			return size;
		}
		case unionArray:
		{
			PVUnionArray pvUnionArray = (PVUnionArray)pvField;
			int arrayLength = pvUnionArray.getLength();
			int size = getSerializationSizeSize(arrayLength);
			UnionArrayData sad = new UnionArrayData();
			pvUnionArray.get(0, arrayLength, sad);
			for (PVUnion s : sad.data)
				size += 1 + getUnionSerializationSize(s);
			return size;
		}
		default:
			throw new IllegalStateException("unsupported type " + pvField.getField());
		}
	}

	public static int getSerializationSizeSize(int size)
	{
		if (size == -1)	
			return 1;
		else if (size < 254)
			return 1;
		else
			return 5;
	}

	public static int getStringSerializationSize(String str)
	{
		int len = str.length();
		return getSerializationSizeSize(len) + len;
	}

	public static int getStructureSerializationSize(PVStructure pvStructure)
	{
		PVField[] pvFields = pvStructure.getPVFields();
		int size = getSerializationSizeSize(pvFields.length);
		for (PVField pvf : pvFields)
			size += getSerializationSize(pvf);
		return size;
	}

	public static int getUnionSerializationSize(PVUnion pvUnion)
	{
		PVField value = pvUnion.get();
		if (pvUnion.getUnion().isVariant())
		{
			if (value == null)
				return 1;
			else
			{
				throw new RuntimeException("not supported");
				// TODO 
				//return getSerializationSize(value.getField()) + getSerializationSize(value);
			}
		}
		else
		{
			int selector = pvUnion.getSelectedIndex();
			int size = getSerializationSizeSize(selector);
			if (selector != PVUnion.UNDEFINED_INDEX) 
				size += getSerializationSize(value);
			return size;
		}
	}

	static class NoopDeserializableControl implements DeserializableControl
	{
		static final FieldCreate fieldCreate = FieldFactory.getFieldCreate();

		@Override
		public void ensureData(int size) {
			// noop
		}

		@Override
		public void alignData(int alignment) {
			// TODO
		}

		@Override
		public Field cachedDeserialize(ByteBuffer buffer) {
			return fieldCreate.deserialize(buffer, this);
		}
	}

	static class NoopSerializableControl implements SerializableControl
	{

		@Override
		public void flushSerializeBuffer() {
			// noop
		}

		@Override
		public void ensureBuffer(int size) {
			// noop
		}

		@Override
		public void alignBuffer(int alignment) {
			// TODO
		}

		@Override
		public void cachedSerialize(Field field, ByteBuffer buffer) {
			field.serialize(buffer, this);
		}
		
	}


}
